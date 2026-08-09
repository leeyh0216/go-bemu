package contract

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// WriteManifest is the generator boundary used by CI and contributors. The
// stable JSON is derived exclusively from literal annotations plus reviewed
// runner-only exceptions.
func WriteManifest(path string, operations []Operation, exceptions []Exception) error {
	if err := ValidateExceptions(exceptions); err != nil {
		return err
	}
	payload := struct {
		Operations []Operation `json:"operations"`
		Exceptions []Exception `json:"exceptions,omitempty"`
	}{operations, exceptions}
	encoded, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(encoded, '\n'), 0o600)
}

// WriteCompatibilityDocuments renders the bilingual, reviewable projection of
// the same source model used by the machine-readable manifest.
func WriteCompatibilityDocuments(englishPath, koreanPath string, operations []Operation, exceptions []Exception) error {
	if err := ValidateExceptions(exceptions); err != nil {
		return err
	}
	for _, document := range []struct {
		path, language, switchPath, heading, introduction, source, exceptionHeading string
	}{
		{englishPath, "en", "../../ko/generated/integration-consumer-contract.md", "Generated Integration Consumer Contract", "This generated inventory is derived from literal integration-test annotations.", "Source annotation", "Runner-only exceptions"},
		{koreanPath, "ko", "../../en/generated/integration-consumer-contract.md", "생성된 통합 소비자 계약", "이 생성 목록은 통합 테스트 원본의 literal annotation에서 파생됩니다.", "원본 annotation", "runner 전용 예외"},
	} {
		if err := os.MkdirAll(filepath.Dir(document.path), 0o755); err != nil {
			return err
		}
		var output strings.Builder
		fmt.Fprintf(&output, "<!-- doc-id: generated/integration-consumer-contract -->\n<!-- lang: %s -->\n\n[English](%s) | [한국어](%s)\n\n", document.language, languageSwitch(document.language, "en", document.switchPath), languageSwitch(document.language, "ko", document.switchPath))
		fmt.Fprintf(&output, "# %s\n\n<!-- section: operations -->\n%s\n\n", document.heading, document.introduction)
		output.WriteString("| Operation | Scenario | " + document.source + " |\n| --- | --- | --- |\n")
		for _, operation := range operations {
			fmt.Fprintf(&output, "| `%s` | `%s` | `%s` |\n", operation.ID, operation.Scenario, operation.Source)
		}
		output.WriteString("\n<!-- section: runner-only-exceptions -->\n## " + document.exceptionHeading + "\n\n")
		if len(exceptions) == 0 {
			if document.language == "ko" {
				output.WriteString("현재 선언된 예외가 없습니다.\n")
			} else {
				output.WriteString("No exceptions are declared.\n")
			}
		} else {
			output.WriteString("| Scenario | Reason |\n| --- | --- |\n")
			for _, exception := range exceptions {
				fmt.Fprintf(&output, "| `%s` | %s |\n", exception.Scenario, exception.Reason)
			}
		}
		output.WriteString("\n<!-- section: provenance -->\n")
		if document.language == "ko" {
			output.WriteString("Operation 이름은 [BigQuery REST reference](https://cloud.google.com/bigquery/docs/reference/rest)를 따릅니다.\n")
		} else {
			output.WriteString("Operation names follow the [BigQuery REST reference](https://cloud.google.com/bigquery/docs/reference/rest).\n")
		}
		if err := os.WriteFile(document.path, []byte(output.String()), 0o600); err != nil {
			return err
		}
	}
	return nil
}

func languageSwitch(language, targetLanguage, otherPath string) string {
	if language == targetLanguage {
		return "."
	}
	return otherPath
}
