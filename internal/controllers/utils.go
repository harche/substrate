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
	atev1alpha1 "github.com/agent-substrate/substrate/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

// createActorDeploymentSpec creates a deployment spec for an actor.
func createActorDeploymentSpec(name string, replicas int32, wpName string, ateomImage string) *appsv1.DeploymentSpec {
	return createActorDeploymentSpecWithRuntime(name, replicas, wpName, ateomImage, atev1alpha1.RuntimeTypeGVisor, "")
}

// createActorDeploymentSpecWithRuntime creates a deployment spec for an actor,
// configured for the specified runtime type.
func createActorDeploymentSpecWithRuntime(name string, replicas int32, wpName string, ateomImage string, runtimeType atev1alpha1.RuntimeType, runtimeClassName string) *appsv1.DeploymentSpec {
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
			Spec: buildPodSpec(ateomImage, runtimeType, runtimeClassName),
		},
	}
	if wpName != "" {
		ds.Template.ObjectMeta.Labels["ate.dev/worker-pool"] = wpName
	}
	return ds
}

// buildPodSpec builds the pod spec appropriate for the given runtime type.
func buildPodSpec(ateomImage string, runtimeType atev1alpha1.RuntimeType, runtimeClassName string) corev1.PodSpec {
	switch runtimeType {
	case atev1alpha1.RuntimeTypeKata:
		return buildKataPodSpec(ateomImage, runtimeClassName)
	default:
		return buildGVisorPodSpec(ateomImage)
	}
}

// buildGVisorPodSpec builds the pod spec for gVisor workers.
// gVisor workers run ateom inside the pod with privileged access to manage
// runsc containers and network namespaces.
func buildGVisorPodSpec(ateomImage string) corev1.PodSpec {
	return corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Name:  "ateom",
				Image: ateomImage,
				Args: []string{
					"-pod-namespace=$(POD_NAMESPACE)",
					"-pod-name=$(POD_NAME)",
				},
				SecurityContext: &corev1.SecurityContext{
					Privileged: ptr.To(true),
					RunAsUser:  ptr.To(int64(0)),
					RunAsGroup: ptr.To(int64(0)),
				},
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
		SecurityContext: &corev1.PodSecurityContext{
			RunAsUser:  ptr.To(int64(0)),
			RunAsGroup: ptr.To(int64(0)),
		},
		Volumes: []corev1.Volume{
			{
				Name: "run-ateom",
				VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{
						Path: "/run/ateom-gvisor",
						Type: ptr.To(corev1.HostPathDirectoryOrCreate),
					},
				},
			},
		},
	}
}

// buildKataPodSpec builds the pod spec for Kata Containers workers.
// Kata workers are VMs — isolation is provided at the VM level, so no
// privileged security context is needed. The RuntimeClassName directs the
// kubelet to create the pod as a Kata micro-VM.
func buildKataPodSpec(ateomImage string, runtimeClassName string) corev1.PodSpec {
	if runtimeClassName == "" {
		runtimeClassName = "kata"
	}

	return corev1.PodSpec{
		RuntimeClassName: &runtimeClassName,
		Containers: []corev1.Container{
			{
				Name:  "ateom",
				Image: ateomImage,
				Args: []string{
					"-pod-namespace=$(POD_NAMESPACE)",
					"-pod-name=$(POD_NAME)",
				},
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
		Volumes: []corev1.Volume{
			{
				Name: "run-ateom",
				VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{
						Path: "/run/ateom-gvisor",
						Type: ptr.To(corev1.HostPathDirectoryOrCreate),
					},
				},
			},
		},
	}
}
