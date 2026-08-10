package integrationcontract

import (
	"bytes"
	"encoding/json"
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

// ConsumerToolMatrixRow connects a canonical public consumer case to the
// Compose service that executes it.  The case data comes from consumers.yaml
// plus its case YAML files; service names remain intentionally limited to the
// audited images in compose.tools.yaml.
type ConsumerToolMatrixRow struct {
	CaseID         string `json:"caseId"`
	ExecutionID    string `json:"executionId"`
	Family         string `json:"family"`
	ComposeService string `json:"composeService"`
}

func renderConsumerToolMatrix(manifest NormalizedConsumerManifest) ([]byte, error) {
	services := map[string]string{
		"python": "consumer-python",
		"bq":     "consumer-bq",
		"spark":  "consumer-spark",
	}
	rows := make([]ConsumerToolMatrixRow, 0)
	for _, consumerCase := range manifest.Cases {
		if consumerCase.Lane != "required" {
			continue
		}
		service, ok := services[consumerCase.Family]
		if !ok {
			return nil, fmt.Errorf("consumer tool matrix has no Compose service for family %s", consumerCase.Family)
		}
		for _, execution := range consumerCase.Executions {
			if execution.ID == "public" {
				rows = append(rows, ConsumerToolMatrixRow{CaseID: consumerCase.ID, ExecutionID: execution.ID, Family: consumerCase.Family, ComposeService: service})
			}
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].CaseID < rows[j].CaseID })
	payload := struct {
		SchemaVersion string                  `json:"schemaVersion"`
		Include       []ConsumerToolMatrixRow `json:"include"`
	}{SchemaVersion: consumerSchemaVersion, Include: rows}
	contents, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode consumer tool matrix: %w", err)
	}
	return append(contents, '\n'), nil
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
	toolMatrix, err := renderConsumerToolMatrix(manifest)
	if err != nil {
		return nil, err
	}
	artifacts := []GeneratedArtifact{
		{Path: consumerNormalizedPath, Contents: normalized},
		{Path: "tests/integration/contract/consumer-tools.matrix.json", Contents: toolMatrix},
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
