package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/leeyh0216/go-bemu/internal/bootstrap"
	"github.com/leeyh0216/go-bemu/internal/observability"
)

// buildVersion is injected from release/version.json by release builds.
var buildVersion = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := bootstrap.Run(ctx, os.Args[1:], os.Stdout, buildVersion); err != nil {
		attrs := []any{"event", "runtime.exit"}
		attrs = append(attrs, observability.ErrorAttrs(err)...)
		slog.Error("BQEMU stopped", attrs...)
		os.Exit(1)
	}
}
