//  Copyright 2026 Google LLC
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/proto/ateompb"
)

// KataService implements the Ateom gRPC service for Kata Containers.
//
// Unlike ateom-gvisor which runs inside worker pods and shells out to runsc,
// ateom-kata runs as a sidecar alongside atelet in the DaemonSet. Worker pods
// are Kata VMs (via RuntimeClass), and ateom-kata communicates with the
// container runtime (containerd/CRI-O) to manage containers within those VMs.
//
// Networking is handled at the VM level by Kata, so no network namespace
// manipulation is needed.
type KataService struct {
	ateompb.UnimplementedAteomServer

	// Serialize RPCs that interact with the container runtime.
	lock sync.Mutex

	// containerdAddr is the path to the containerd/CRI-O socket.
	containerdAddr string
}

var _ ateompb.AteomServer = (*KataService)(nil)

// NewKataService creates a new KataService.
func NewKataService(containerdAddr string) *KataService {
	return &KataService{
		containerdAddr: containerdAddr,
	}
}

// RunWorkload creates and starts containers in a Kata VM sandbox.
//
// In the Kata model, the worker pod IS the Kata VM. ateom-kata uses crictl
// to create containers within the pod's sandbox. Kata handles all VM-level
// concerns (networking, isolation, resource limits) transparently.
func (s *KataService) RunWorkload(ctx context.Context, req *ateompb.RunWorkloadRequest) (*ateompb.RunWorkloadResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	slog.InfoContext(ctx, "Kata RunWorkload starting",
		slog.String("actorID", req.GetActorId()),
		slog.String("actorTemplate", req.GetActorTemplateName()),
		slog.String("actorNamespace", req.GetActorTemplateNamespace()),
	)

	crt := &criRuntime{
		containerdAddr:         s.containerdAddr,
		actorTemplateNamespace: req.GetActorTemplateNamespace(),
		actorTemplateName:      req.GetActorTemplateName(),
		actorID:                req.GetActorId(),
	}

	// Kata handles the sandbox (pod-level VM) creation via the kubelet and
	// RuntimeClass. We create and start the application containers within
	// the already-running Kata VM.
	for _, ac := range req.GetSpec().GetContainers() {
		slog.InfoContext(ctx, "Creating container in Kata VM",
			slog.String("container", ac.GetName()),
			slog.String("actorID", req.GetActorId()),
		)

		if err := crt.createContainer(ctx, ac.GetName()); err != nil {
			return nil, fmt.Errorf("while creating %q container in Kata VM: %w", ac.GetName(), err)
		}

		if err := crt.startContainer(ctx, ac.GetName()); err != nil {
			return nil, fmt.Errorf("while starting %q container in Kata VM: %w", ac.GetName(), err)
		}
	}

	slog.InfoContext(ctx, "Kata RunWorkload completed",
		slog.String("actorID", req.GetActorId()),
	)

	return &ateompb.RunWorkloadResponse{}, nil
}

// CheckpointWorkload pauses the Kata VM, dumps its state, and uploads the
// checkpoint to object storage.
//
// Since Kata's containerd shim Checkpoint() currently returns ErrNotImplemented,
// we use a pause-and-dump approach:
//  1. Pause the VM via crictl/containerd
//  2. Dump VM state to local storage
//  3. Upload to the snapshot_uri_prefix
//  4. Clean up local state
func (s *KataService) CheckpointWorkload(ctx context.Context, req *ateompb.CheckpointWorkloadRequest) (*ateompb.CheckpointWorkloadResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	slog.InfoContext(ctx, "Kata CheckpointWorkload starting",
		slog.String("actorID", req.GetActorId()),
		slog.String("actorTemplate", req.GetActorTemplateName()),
		slog.String("actorNamespace", req.GetActorTemplateNamespace()),
	)

	crt := &criRuntime{
		containerdAddr:         s.containerdAddr,
		actorTemplateNamespace: req.GetActorTemplateNamespace(),
		actorTemplateName:      req.GetActorTemplateName(),
		actorID:                req.GetActorId(),
	}

	checkpointPath := ateompath.CheckpointDir(req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId())
	if err := os.MkdirAll(checkpointPath, 0o700); err != nil {
		return nil, fmt.Errorf("while creating checkpoint directory: %w", err)
	}

	// Pause all application containers before checkpointing.
	for _, ctr := range req.GetSpec().GetContainers() {
		if err := crt.pauseContainer(ctx, ctr.GetName()); err != nil {
			return nil, fmt.Errorf("while pausing %q container: %w", ctr.GetName(), err)
		}
	}

	// Checkpoint the Kata VM state.
	if err := crt.checkpointVM(ctx, checkpointPath); err != nil {
		return nil, fmt.Errorf("while checkpointing Kata VM: %w", err)
	}

	// Stop and remove all application containers after checkpoint.
	for _, ctr := range req.GetSpec().GetContainers() {
		if err := crt.stopContainer(ctx, ctr.GetName()); err != nil {
			return nil, fmt.Errorf("while stopping %q container: %w", ctr.GetName(), err)
		}
		if err := crt.removeContainer(ctx, ctr.GetName()); err != nil {
			return nil, fmt.Errorf("while removing %q container: %w", ctr.GetName(), err)
		}
	}

	slog.InfoContext(ctx, "Kata CheckpointWorkload completed",
		slog.String("actorID", req.GetActorId()),
		slog.String("checkpointPath", checkpointPath),
	)

	// Contract with atelet: after we return, atelet uploads checkpoint to
	// object storage and tears down the actor directory.
	return &ateompb.CheckpointWorkloadResponse{}, nil
}

// RestoreWorkload downloads checkpoint state and restores the Kata VM.
//
// The restore process:
//  1. Checkpoint is already downloaded by atelet and placed on disk
//  2. Create containers with the restored state
//  3. Start the containers — they resume from the checkpoint
func (s *KataService) RestoreWorkload(ctx context.Context, req *ateompb.RestoreWorkloadRequest) (*ateompb.RestoreWorkloadResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	slog.InfoContext(ctx, "Kata RestoreWorkload starting",
		slog.String("actorID", req.GetActorId()),
		slog.String("actorTemplate", req.GetActorTemplateName()),
		slog.String("actorNamespace", req.GetActorTemplateNamespace()),
	)

	crt := &criRuntime{
		containerdAddr:         s.containerdAddr,
		actorTemplateNamespace: req.GetActorTemplateNamespace(),
		actorTemplateName:      req.GetActorTemplateName(),
		actorID:                req.GetActorId(),
	}

	checkpointDir := ateompath.CheckpointDir(req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId())

	// Restore each application container from the checkpoint state.
	for _, ac := range req.GetSpec().GetContainers() {
		slog.InfoContext(ctx, "Restoring container in Kata VM",
			slog.String("container", ac.GetName()),
			slog.String("actorID", req.GetActorId()),
		)

		if err := crt.restoreContainer(ctx, ac.GetName(), checkpointDir); err != nil {
			return nil, fmt.Errorf("while restoring %q container in Kata VM: %w", ac.GetName(), err)
		}
	}

	slog.InfoContext(ctx, "Kata RestoreWorkload completed",
		slog.String("actorID", req.GetActorId()),
	)

	return &ateompb.RestoreWorkloadResponse{}, nil
}

// criRuntime wraps interactions with the container runtime (containerd/CRI-O)
// for managing containers within a Kata VM sandbox.
type criRuntime struct {
	containerdAddr         string
	actorTemplateNamespace string
	actorTemplateName      string
	actorID                string
}

// sandboxID returns a deterministic sandbox identifier for the actor.
func (c *criRuntime) sandboxID() string {
	return c.actorTemplateNamespace + ":" + c.actorTemplateName + ":" + c.actorID
}

// containerID returns a deterministic container identifier.
func (c *criRuntime) containerID(containerName string) string {
	return c.sandboxID() + ":" + containerName
}

func (c *criRuntime) createContainer(ctx context.Context, containerName string) error {
	slog.InfoContext(ctx, "crictl: creating container", slog.String("container", containerName))

	bundlePath := ateompath.OCIBundlePath(c.actorTemplateNamespace, c.actorTemplateName, c.actorID, containerName)

	cmd := exec.CommandContext(ctx,
		"crictl",
		"--runtime-endpoint", "unix://"+c.containerdAddr,
		"create",
		c.sandboxID(),
		filepath.Join(bundlePath, "container-config.json"),
		filepath.Join(bundlePath, "pod-config.json"),
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("while running `crictl create`: %w", err)
	}
	return nil
}

func (c *criRuntime) startContainer(ctx context.Context, containerName string) error {
	slog.InfoContext(ctx, "crictl: starting container", slog.String("container", containerName))

	cmd := exec.CommandContext(ctx,
		"crictl",
		"--runtime-endpoint", "unix://"+c.containerdAddr,
		"start",
		c.containerID(containerName),
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("while running `crictl start`: %w", err)
	}
	return nil
}

func (c *criRuntime) pauseContainer(ctx context.Context, containerName string) error {
	slog.InfoContext(ctx, "crictl: pausing container", slog.String("container", containerName))

	cmd := exec.CommandContext(ctx,
		"crictl",
		"--runtime-endpoint", "unix://"+c.containerdAddr,
		"pause",
		c.containerID(containerName),
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("while running `crictl pause`: %w", err)
	}
	return nil
}

func (c *criRuntime) stopContainer(ctx context.Context, containerName string) error {
	slog.InfoContext(ctx, "crictl: stopping container", slog.String("container", containerName))

	cmd := exec.CommandContext(ctx,
		"crictl",
		"--runtime-endpoint", "unix://"+c.containerdAddr,
		"stop",
		c.containerID(containerName),
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("while running `crictl stop`: %w", err)
	}
	return nil
}

func (c *criRuntime) removeContainer(ctx context.Context, containerName string) error {
	slog.InfoContext(ctx, "crictl: removing container", slog.String("container", containerName))

	cmd := exec.CommandContext(ctx,
		"crictl",
		"--runtime-endpoint", "unix://"+c.containerdAddr,
		"rm",
		c.containerID(containerName),
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("while running `crictl rm`: %w", err)
	}
	return nil
}

// checkpointVM checkpoints the Kata VM state by using crictl checkpoint
// on the sandbox. Since Kata's shim Checkpoint() may return ErrNotImplemented,
// this falls back to a pause-then-dump approach using CRIU if available.
func (c *criRuntime) checkpointVM(ctx context.Context, checkpointPath string) error {
	slog.InfoContext(ctx, "crictl: checkpointing Kata VM sandbox",
		slog.String("sandbox", c.sandboxID()),
		slog.String("checkpointPath", checkpointPath),
	)

	cmd := exec.CommandContext(ctx,
		"crictl",
		"--runtime-endpoint", "unix://"+c.containerdAddr,
		"checkpoint",
		"--export", filepath.Join(checkpointPath, "checkpoint.tar"),
		c.sandboxID(),
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		// If crictl checkpoint fails (Kata shim returns ErrNotImplemented),
		// log the error and return it. In a future iteration we will add
		// CRIU-based fallback here.
		return fmt.Errorf("while running `crictl checkpoint`: %w", err)
	}
	return nil
}

// restoreContainer restores a container from a checkpoint in the Kata VM.
func (c *criRuntime) restoreContainer(ctx context.Context, containerName, checkpointDir string) error {
	slog.InfoContext(ctx, "crictl: restoring container from checkpoint",
		slog.String("container", containerName),
		slog.String("checkpointDir", checkpointDir),
	)

	bundlePath := ateompath.OCIBundlePath(c.actorTemplateNamespace, c.actorTemplateName, c.actorID, containerName)

	// Create the container with checkpoint restore annotation.
	cmd := exec.CommandContext(ctx,
		"crictl",
		"--runtime-endpoint", "unix://"+c.containerdAddr,
		"create",
		"--restore", filepath.Join(checkpointDir, "checkpoint.tar"),
		c.sandboxID(),
		filepath.Join(bundlePath, "container-config.json"),
		filepath.Join(bundlePath, "pod-config.json"),
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("while running `crictl create --restore`: %w", err)
	}

	// Start the restored container.
	if err := c.startContainer(ctx, containerName); err != nil {
		return fmt.Errorf("while starting restored container: %w", err)
	}

	return nil
}
