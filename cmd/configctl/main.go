// configctl generates checked-in configuration artifacts from the versioned
// Config model. It is intentionally small so CI can reproduce outputs without
// a code generator dependency.
package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/leeyh0216/go-bemu/internal/config"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) != 1 || (args[0] != "generate" && args[0] != "check") {
		return errors.New("usage: configctl generate|check")
	}
	artifacts := map[string]func() ([]byte, error){
		"contract/config.schema.json": config.JSONSchema,
		"docs/en/config-reference.md": func() ([]byte, error) { value, err := config.ConfigReferenceMarkdown("en"); return []byte(value), err },
		"docs/ko/config-reference.md": func() ([]byte, error) { value, err := config.ConfigReferenceMarkdown("ko"); return []byte(value), err },
	}
	for path, render := range artifacts {
		want, err := render()
		if err != nil {
			return err
		}
		if args[0] == "generate" {
			if err := os.WriteFile(filepath.Clean(path), append(want, '\n'), 0o644); err != nil {
				return err
			}
			continue
		}
		got, err := os.ReadFile(filepath.Clean(path))
		if err != nil {
			return err
		}
		if string(got) != string(append(want, '\n')) {
			return fmt.Errorf("generated configuration artifact is stale: %s (run go run ./cmd/configctl generate)", path)
		}
	}
	return nil
}
