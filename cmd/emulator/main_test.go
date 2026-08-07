package main

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/leeyh0216/go-bemu/internal/config"
)

func TestRunPrintEffectiveConfigDoesNotStartListeners(t *testing.T) {
	var output bytes.Buffer
	err := run(t.Context(), []string{
		"--set", "defaults.projectId=test-project",
		"--set", "server.http.address=invalid-listener-address",
		"--print-effective-config",
	}, &output)
	if err == nil || !strings.Contains(err.Error(), "server.http.address") {
		// Effective output still validates before it is printed; an invalid
		// address must fail without attempting any network side effect.
		if err == nil {
			t.Fatal("expected invalid effective configuration")
		}
		t.Fatalf("unexpected error: %v", err)
	}

	output.Reset()
	if err := run(t.Context(), []string{
		"--set", "defaults.projectId=test-project",
		"--set", "database.dsn=:memory:",
		"--print-effective-config",
	}, &output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "projectId: test-project") || !strings.Contains(output.String(), "dsn: ':memory:'") {
		t.Fatalf("unexpected effective configuration:\n%s", output.String())
	}
}

func TestConfigureLoggerAppliesFormatAndLevel(t *testing.T) {
	var output bytes.Buffer
	logger, err := configureLogger(config.LoggingConfig{Level: "warn", Format: "json"}, &output)
	if err != nil {
		t.Fatal(err)
	}
	logger.Info("filtered")
	logger.Warn("retained", "model_version", config.APIVersion)
	encoded := output.String()
	if strings.Contains(encoded, "filtered") || !strings.Contains(encoded, `"msg":"retained"`) || !strings.Contains(encoded, config.APIVersion) {
		t.Fatalf("unexpected log output: %s", encoded)
	}
	if _, err := configureLogger(config.LoggingConfig{Level: "trace", Format: "json"}, &output); err == nil {
		t.Fatal("expected unsupported level error")
	}
	if _, err := configureLogger(config.LoggingConfig{Level: "info", Format: "xml"}, &output); err == nil {
		t.Fatal("expected unsupported format error")
	}
}

func TestPrepareDirectoryCreatesConfiguredPath(t *testing.T) {
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	directory := filepath.Join(t.TempDir(), "nested", "bqemu")
	if err := prepareDirectory(context.Background(), directory); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(directory)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() {
		t.Fatalf("configured path is not a directory: %s", directory)
	}
}
