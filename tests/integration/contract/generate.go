package integrationcontract

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type GeneratedArtifact struct {
	Path     string
	Contents []byte
}

func CompileArtifacts(repositoryRoot string) ([]GeneratedArtifact, error) {
	manifest, err := CompileConsumerManifest(repositoryRoot)
	if err != nil {
		return nil, err
	}
	normalized, err := MarshalNormalizedConsumerManifest(manifest)
	if err != nil {
		return nil, fmt.Errorf("encode normalized consumer manifest: %w", err)
	}
	artifacts := []GeneratedArtifact{
		{Path: consumerNormalizedPath, Contents: normalized},
		{Path: "tests/integration/docs/en/consumer-compatibility.md", Contents: renderConsumerCompatibility(manifest, "en")},
		{Path: "tests/integration/docs/ko/consumer-compatibility.md", Contents: renderConsumerCompatibility(manifest, "ko")},
	}
	sort.Slice(artifacts, func(i, j int) bool { return artifacts[i].Path < artifacts[j].Path })
	return artifacts, nil
}

func WriteArtifacts(repositoryRoot string) error {
	artifacts, err := CompileArtifacts(repositoryRoot)
	if err != nil {
		return err
	}
	for _, artifact := range artifacts {
		path := filepath.Join(repositoryRoot, artifact.Path)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return fmt.Errorf("create directory for %s: %w", artifact.Path, err)
		}
		if err := os.WriteFile(path, artifact.Contents, 0o644); err != nil {
			return fmt.Errorf("write %s: %w", artifact.Path, err)
		}
	}
	return nil
}

func CheckArtifacts(repositoryRoot string) error {
	artifacts, err := CompileArtifacts(repositoryRoot)
	if err != nil {
		return err
	}
	var dirty []string
	for _, artifact := range artifacts {
		actual, err := os.ReadFile(filepath.Join(repositoryRoot, artifact.Path))
		if err != nil || !bytes.Equal(actual, artifact.Contents) {
			dirty = append(dirty, artifact.Path)
		}
	}
	if len(dirty) != 0 {
		return fmt.Errorf("generated integration artifacts are stale: %s; run `make integration-contract-generate`", strings.Join(dirty, ", "))
	}
	return nil
}
