package main

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/leeyh0216/go-bemu/internal/release"
)

func TestPrepare(t *testing.T) {
	path := filepath.Join(t.TempDir(), "version.json")
	if err := release.Write(path, release.Descriptor{Version: "0.1.0"}); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := run([]string{"prepare", "--file", path, "--bump", "minor"}, &output); err != nil {
		t.Fatal(err)
	}
	if output.String() != "0.2.0\n" {
		t.Fatalf("output = %q", output.String())
	}
	got, err := release.Read(path)
	if err != nil || got.Version != "0.2.0" {
		t.Fatalf("version = %#v, %v", got, err)
	}
}

func TestCheckRejectsRegression(t *testing.T) {
	path := filepath.Join(t.TempDir(), "version.json")
	if err := release.Write(path, release.Descriptor{Version: "0.9.0"}); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"check", "--file", path, "--base-version", "0.10.0"}, &bytes.Buffer{}); err == nil {
		t.Fatal("regression check succeeded")
	}
}
