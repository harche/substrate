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
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"

	"github.com/agent-substrate/substrate/internal/ateinterceptors"
	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/contextlogging"
	"github.com/agent-substrate/substrate/proto/ateompb"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var (
	podNamespace    = flag.String("pod-namespace", "", "The namespace of the current pod")
	podName         = flag.String("pod-name", "", "The name of the current pod")
	containerdAddr  = flag.String("containerd-socket", "/run/containerd/containerd.sock", "Path to the containerd/CRI-O socket")
)

func main() {
	flag.Parse()
	ctx := context.Background()

	if err := do(ctx); err != nil {
		slog.ErrorContext(ctx, "Error while executing", slog.Any("err", err))
		os.Exit(1)
	}
}

func do(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	logger := slog.New(contextlogging.NewHandler(slog.NewJSONHandler(os.Stdout, nil)))
	slog.SetDefault(logger)

	slog.InfoContext(ctx, "ateom-kata booting")

	tp, err := initTracing(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to initialize tracing", slog.Any("err", err))
		os.Exit(1)
	}
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			slog.Error("Failed to shutdown TracerProvider", slog.Any("err", err))
		}
	}()

	// Create ateom dir
	ateomDir := ateompath.AteomPath(*podNamespace, *podName)
	if err := os.MkdirAll(ateomDir, 0o700); err != nil {
		return fmt.Errorf("in os.MkdirAll(%q): %w", ateomDir, err)
	}

	// Clean up any old socket.
	sockPath := ateompath.AteomSocketPath(*podNamespace, *podName)
	if err := os.RemoveAll(sockPath); err != nil {
		return fmt.Errorf("while removing %q: %w", sockPath, err)
	}

	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		return fmt.Errorf("while opening unix socket: %w", err)
	}

	kataService := NewKataService(*containerdAddr)

	svr := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.UnaryInterceptor(ateinterceptors.ServerUnaryInterceptor),
	)
	ateompb.RegisterAteomServer(svr, kataService)
	reflection.Register(svr)

	slog.InfoContext(ctx, "ateom-kata serving", slog.String("socket", sockPath), slog.String("containerd", *containerdAddr))

	if err := svr.Serve(lis); err != nil {
		slog.ErrorContext(ctx, "Failed to serve", slog.Any("err", err))
		os.Exit(1)
	}

	return nil
}

func initTracing(ctx context.Context) (*sdktrace.TracerProvider, error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName("ateom-kata"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		// Only trace on-demand when signaled by the client (e.g. via --trace flag)
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.NeverSample())),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp, nil
}
