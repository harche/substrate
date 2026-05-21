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
	"sync"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/proto/ateompb"
)

// CriuService implements the Ateom gRPC service using runc + CRIU for
// checkpoint/restore of regular Linux containers.
//
// Unlike the gVisor backend, CRIU does not need:
//   - Network namespace manipulation (containers use pod networking directly)
//   - A child process reaper (runc manages its own children)
//   - The runsc -root state directory (runc uses /run/runc by default)
type CriuService struct {
	ateompb.UnimplementedAteomServer

	// Ateom RPCs that run runc subcommands are not safe to call concurrently.
	lock sync.Mutex

	runcPath    string
	actorLogger *ActorLogger
}

var _ ateompb.AteomServer = (*CriuService)(nil)

// NewCriuService creates a new CriuService.
func NewCriuService(runcPath string, actorLogger *ActorLogger) *CriuService {
	return &CriuService{
		runcPath:    runcPath,
		actorLogger: actorLogger,
	}
}

func (s *CriuService) RunWorkload(ctx context.Context, req *ateompb.RunWorkloadRequest) (*ateompb.RunWorkloadResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.actorLogger.EmitLifecycleLog("Actor starting", req.GetActorId(), req.GetActorTemplateName(), req.GetActorTemplateNamespace())

	// Contract with atelet:
	//
	//   * All OCI bundles are set up, including for "pause" container.
	//   * For CRIU, no runsc binary download needed — runc is pre-installed.

	rcmd := &runcCmd{
		path:                   s.runcPath,
		actorTemplateNamespace: req.GetActorTemplateNamespace(),
		actorTemplateName:      req.GetActorTemplateName(),
		actorID:                req.GetActorId(),
	}

	// Create and start pause container
	if err := rcmd.cmdCreate(ctx, os.Stdout, "pause"); err != nil {
		return nil, fmt.Errorf("while creating pause container: %w", err)
	}
	if err := rcmd.cmdStart(ctx, os.Stdout, "pause"); err != nil {
		return nil, fmt.Errorf("while starting pause container: %w", err)
	}

	pw, err := s.actorLogger.StartJSONLogPipe(req.GetActorId(), req.GetActorTemplateName(), req.GetActorTemplateNamespace())
	if err != nil {
		return nil, fmt.Errorf("while starting json log pipe: %w", err)
	}
	defer pw.Close()

	// Create and start each application container
	for _, ac := range req.GetSpec().GetContainers() {
		if err := rcmd.cmdCreate(ctx, pw, ac.GetName()); err != nil {
			return nil, fmt.Errorf("while creating %q application container: %w", ac.GetName(), err)
		}
		if err := rcmd.cmdStart(ctx, pw, ac.GetName()); err != nil {
			return nil, fmt.Errorf("while starting %q application container: %w", ac.GetName(), err)
		}
	}

	s.actorLogger.EmitLifecycleLog("Actor started", req.GetActorId(), req.GetActorTemplateName(), req.GetActorTemplateNamespace())

	return &ateompb.RunWorkloadResponse{}, nil
}

func (s *CriuService) CheckpointWorkload(ctx context.Context, req *ateompb.CheckpointWorkloadRequest) (*ateompb.CheckpointWorkloadResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.actorLogger.EmitLifecycleLog("Actor checkpointing", req.GetActorId(), req.GetActorTemplateName(), req.GetActorTemplateNamespace())

	// Contract with atelet:
	//
	//   * After we exit, atelet will upload checkpoint to object storage.
	//   * After we exit, atelet will tear down OCI bundles and reset the actor directory.

	rcmd := &runcCmd{
		path:                   s.runcPath,
		actorTemplateNamespace: req.GetActorTemplateNamespace(),
		actorTemplateName:      req.GetActorTemplateName(),
		actorID:                req.GetActorId(),
	}

	checkpointPath := ateompath.CheckpointDir(req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId())
	if err := os.MkdirAll(checkpointPath, 0o700); err != nil {
		return nil, fmt.Errorf("while creating checkpoint directory: %w", err)
	}

	// CRIU checkpoints each container individually. Checkpoint application
	// containers first (reverse of startup order), then pause.
	containers := req.GetSpec().GetContainers()
	for i := len(containers) - 1; i >= 0; i-- {
		ctr := containers[i]
		slog.InfoContext(ctx, "Checkpointing application container", slog.String("container", ctr.GetName()))
		if err := rcmd.cmdCheckpoint(ctx, ctr.GetName(), checkpointPath); err != nil {
			return nil, fmt.Errorf("while checkpointing %q application container: %w", ctr.GetName(), err)
		}
	}

	// Checkpoint pause container
	slog.InfoContext(ctx, "Checkpointing pause container")
	if err := rcmd.cmdCheckpoint(ctx, "pause", checkpointPath); err != nil {
		return nil, fmt.Errorf("while checkpointing pause: %w", err)
	}

	// Delete all application containers
	for _, ctr := range containers {
		if err := rcmd.cmdDelete(ctx, ctr.GetName()); err != nil {
			return nil, fmt.Errorf("while deleting %q application container: %w", ctr.GetName(), err)
		}
	}

	// Delete pause container
	if err := rcmd.cmdDelete(ctx, "pause"); err != nil {
		return nil, fmt.Errorf("while deleting pause container: %w", err)
	}

	s.actorLogger.EmitLifecycleLog("Actor checkpointed", req.GetActorId(), req.GetActorTemplateName(), req.GetActorTemplateNamespace())

	return &ateompb.CheckpointWorkloadResponse{}, nil
}

func (s *CriuService) RestoreWorkload(ctx context.Context, req *ateompb.RestoreWorkloadRequest) (*ateompb.RestoreWorkloadResponse, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.actorLogger.EmitLifecycleLog("Actor restoring", req.GetActorId(), req.GetActorTemplateName(), req.GetActorTemplateNamespace())

	// Contract with atelet:
	//
	//   * All OCI bundles are set up, including for "pause" container.
	//   * Checkpoint downloaded and placed on disk.

	rcmd := &runcCmd{
		path:                   s.runcPath,
		actorTemplateNamespace: req.GetActorTemplateNamespace(),
		actorTemplateName:      req.GetActorTemplateName(),
		actorID:                req.GetActorId(),
	}

	checkpointDir := ateompath.CheckpointDir(req.GetActorTemplateNamespace(), req.GetActorTemplateName(), req.GetActorId())

	// Restore pause container first
	if err := rcmd.cmdRestore(ctx, os.Stdout, "pause", checkpointDir); err != nil {
		return nil, fmt.Errorf("while restoring pause container: %w", err)
	}

	pw, err := s.actorLogger.StartJSONLogPipe(req.GetActorId(), req.GetActorTemplateName(), req.GetActorTemplateNamespace())
	if err != nil {
		return nil, fmt.Errorf("while starting json log pipe: %w", err)
	}
	defer pw.Close()

	// Restore each application container
	for _, ac := range req.GetSpec().GetContainers() {
		if err := rcmd.cmdRestore(ctx, pw, ac.GetName(), checkpointDir); err != nil {
			return nil, fmt.Errorf("while restoring %q application container: %w", ac.GetName(), err)
		}
	}

	s.actorLogger.EmitLifecycleLog("Actor restored", req.GetActorId(), req.GetActorTemplateName(), req.GetActorTemplateNamespace())

	return &ateompb.RestoreWorkloadResponse{}, nil
}
