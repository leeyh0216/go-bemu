// configctl generates checked-in configuration artifacts from the versioned
// Config model. It is intentionally small so CI can reproduce outputs without
// a code generator dependency.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
		expected := append(want, '\n')
		if generatedArtifactEqual(path, got, expected) {
			continue
		}
		return fmt.Errorf(
			"generated configuration artifact is stale: %s (got sha256:%s, want sha256:%s; run go run ./cmd/configctl generate)",
			path, digest(got), digest(expected),
		)
	}
	return nil
}

// JSON schema is a semantic contract. Go releases may make harmless choices
// about indentation or escaped characters, so schema verification compares the
// decoded JSON value while the human-authored Markdown references remain byte
// exact. A semantic change still fails with content digests for diagnosis.
func generatedArtifactEqual(path string, got, want []byte) bool {
	if filepath.Ext(path) != ".json" {
		return bytes.Equal(got, want)
	}
	var gotValue, wantValue any
	if json.Unmarshal(got, &gotValue) != nil || json.Unmarshal(want, &wantValue) != nil {
		return bytes.Equal(got, want)
	}
	gotCanonical, gotErr := json.Marshal(gotValue)
	wantCanonical, wantErr := json.Marshal(wantValue)
	return gotErr == nil && wantErr == nil && bytes.Equal(gotCanonical, wantCanonical)
}

func digest(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}
