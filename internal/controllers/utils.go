// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controllers

import (
	v1alpha1 "github.com/agent-substrate/substrate/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

// createActorDeploymentSpec creates a deployment spec for an actor.
func createActorDeploymentSpec(name string, replicas int32, wpName string, ateomImage string) *appsv1.DeploymentSpec {
	return createActorDeploymentSpecForRuntime(name, replicas, wpName, ateomImage, v1alpha1.RuntimeTypeGVisor)
}

// createActorDeploymentSpecForRuntime creates a deployment spec for an actor
// with the specified runtime backend.
func ateomCommand(runtimeType v1alpha1.RuntimeType) []string {
	switch runtimeType {
	case v1alpha1.RuntimeTypeCRIU:
		return []string{"/usr/local/bin/ateom-criu"}
	default:
		return nil // use image ENTRYPOINT (ateom-gvisor)
	}
}

func ateomPodSecurityContext(runtimeType v1alpha1.RuntimeType) *corev1.PodSecurityContext {
	if runtimeType == v1alpha1.RuntimeTypeCRIU {
		return nil // CRIU doesn't need root pod context
	}
	return &corev1.PodSecurityContext{
		RunAsUser:  ptr.To(int64(0)),
		RunAsGroup: ptr.To(int64(0)),
	}
}

func ateomVolumes(runtimeType v1alpha1.RuntimeType) []corev1.Volume {
	if runtimeType == v1alpha1.RuntimeTypeCRIU {
		return []corev1.Volume{
			{
				Name: "run-ateom",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				},
			},
		}
	}
	return []corev1.Volume{
		{
			Name: "run-ateom",
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: "/run/ateom-gvisor",
					Type: ptr.To(corev1.HostPathDirectoryOrCreate),
				},
			},
		},
	}
}

func createActorDeploymentSpecForRuntime(name string, replicas int32, wpName string, ateomImage string, runtimeType v1alpha1.RuntimeType) *appsv1.DeploymentSpec {
	secCtx := ateomSecurityContext(runtimeType)
	cmd := ateomCommand(runtimeType)

	ds := &appsv1.DeploymentSpec{
		Replicas: &replicas,
		Selector: &metav1.LabelSelector{
			MatchLabels: map[string]string{
				"app": name,
			},
		},
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					"app": name,
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:    "ateom",
						Image:   ateomImage,
						Command: cmd,
						Args: []string{
							"-pod-namespace=$(POD_NAMESPACE)",
							"-pod-name=$(POD_NAME)",
						},
						SecurityContext: secCtx,
						Env: []corev1.EnvVar{
							{
								Name: "POD_NAMESPACE",
								ValueFrom: &corev1.EnvVarSource{
									FieldRef: &corev1.ObjectFieldSelector{
										FieldPath: "metadata.namespace",
									},
								},
							},
							{
								Name: "POD_NAME",
								ValueFrom: &corev1.EnvVarSource{
									FieldRef: &corev1.ObjectFieldSelector{
										FieldPath: "metadata.name",
									},
								},
							},
						},
						VolumeMounts: []corev1.VolumeMount{
							{
								Name:      "run-ateom",
								MountPath: "/run/ateom-gvisor",
							},
						},
					},
				},
				SecurityContext: ateomPodSecurityContext(runtimeType),
				Volumes:         ateomVolumes(runtimeType),
			},
		},
	}
	if wpName != "" {
		ds.Template.ObjectMeta.Labels["ate.dev/worker-pool"] = wpName
	}
	return ds
}

// ateomSecurityContext returns the appropriate security context for the ateom
// container based on the runtime type.
//
// gVisor requires full privileged mode (for network namespace manipulation
// and sandboxed execution).
//
// CRIU needs SYS_PTRACE and SYS_ADMIN capabilities for checkpoint/restore
// but does not require full privileged mode.
func ateomSecurityContext(runtimeType v1alpha1.RuntimeType) *corev1.SecurityContext {
	if runtimeType == v1alpha1.RuntimeTypeCRIU {
		return &corev1.SecurityContext{
			RunAsUser:  ptr.To(int64(0)),
			RunAsGroup: ptr.To(int64(0)),
			Capabilities: &corev1.Capabilities{
				Add: []corev1.Capability{
					"SYS_PTRACE",
					"SYS_ADMIN",
				},
			},
		}
	}

	// Default: gVisor — full privileged mode
	return &corev1.SecurityContext{
		Privileged: ptr.To(true),
		RunAsUser:  ptr.To(int64(0)),
		RunAsGroup: ptr.To(int64(0)),
	}
}
