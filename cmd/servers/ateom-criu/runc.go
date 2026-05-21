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
	"io"
	"log/slog"
	"os"
	"os/exec"

	"github.com/agent-substrate/substrate/internal/ateompath"
)

// runcCmd wraps the runc CLI binary for container lifecycle operations.
//
// Key differences from runsc (gVisor):
//   - No -root flag needed (runc uses default /run/runc state directory)
//   - No -allow-connected-on-save (CRIU handles TCP connections via TCP_REPAIR)
//   - No -log-format json (runc uses different logging flags)
//   - Checkpoint produces a directory of CRIU image files (not a single file)
//   - Restore uses --image-path pointing to the CRIU image directory
type runcCmd struct {
	path                   string
	actorTemplateNamespace string
	actorTemplateName      string
	actorID                string
}

func (r *runcCmd) cmdCreate(ctx context.Context, out io.Writer, containerName string) error {
	slog.InfoContext(ctx, "About to run runc create", slog.String("container", containerName))

	bundlePath := ateompath.OCIBundlePath(r.actorTemplateNamespace, r.actorTemplateName, r.actorID, containerName)
	pidFilePath := ateompath.PIDFilePath(r.actorTemplateNamespace, r.actorTemplateName, r.actorID, containerName)

	cmd := exec.CommandContext(
		ctx,
		r.path,
		"create",
		"--bundle", bundlePath,
		"--pid-file", pidFilePath,
		containerName,
	)
	cmd.Stdout = out
	cmd.Stderr = out

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("while running `runc create`: %w", err)
	}

	return nil
}

func (r *runcCmd) cmdStart(ctx context.Context, out io.Writer, containerName string) error {
	slog.InfoContext(ctx, "About to run runc start", slog.String("container", containerName))

	cmd := exec.CommandContext(
		ctx,
		r.path,
		"start",
		containerName,
	)
	cmd.Stdout = out
	cmd.Stderr = out

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("while running `runc start`: %w", err)
	}

	return nil
}

func (r *runcCmd) cmdCheckpoint(ctx context.Context, containerName, checkpointPath string) error {
	slog.InfoContext(ctx, "About to run runc checkpoint", slog.String("container", containerName))

	cmd := exec.CommandContext(
		ctx,
		r.path,
		"checkpoint",
		"--image-path", checkpointPath,
		containerName,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("while running `runc checkpoint`: %w", err)
	}

	return nil
}

func (r *runcCmd) cmdRestore(ctx context.Context, out io.Writer, containerName, checkpointPath string) error {
	slog.InfoContext(ctx, "About to run runc restore", slog.String("container", containerName))

	bundlePath := ateompath.OCIBundlePath(r.actorTemplateNamespace, r.actorTemplateName, r.actorID, containerName)
	pidFilePath := ateompath.PIDFilePath(r.actorTemplateNamespace, r.actorTemplateName, r.actorID, containerName)

	cmd := exec.CommandContext(
		ctx,
		r.path,
		"restore",
		"--image-path", checkpointPath,
		"--bundle", bundlePath,
		"--pid-file", pidFilePath,
		"--detach",
		containerName,
	)
	cmd.Stdout = out
	cmd.Stderr = out

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("while running `runc restore`: %w", err)
	}

	return nil
}

func (r *runcCmd) cmdDelete(ctx context.Context, containerName string) error {
	slog.InfoContext(ctx, "About to run runc delete", slog.String("container", containerName))

	cmd := exec.CommandContext(
		ctx,
		r.path,
		"delete",
		"--force",
		containerName,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("while running `runc delete`: %w", err)
	}

	return nil
}

func (r *runcCmd) cmdState(ctx context.Context, containerName string) error {
	slog.InfoContext(ctx, "About to run runc state", slog.String("container", containerName))

	cmd := exec.CommandContext(
		ctx,
		r.path,
		"state",
		containerName,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("while running `runc state`: %w", err)
	}

	return nil
}
