// contract-source-check reports whether every canonical operation source still
// resolves. It is run separately from static manifest compilation because it
// deliberately observes external upstream availability.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/leeyh0216/go-bemu/contract"
)

func main() {
	manifestPath := flag.String("manifest", "contract/operations.yaml", "canonical operation manifest")
	outputPath := flag.String("output", "", "optional JSON report path")
	flag.Parse()
	if err := run(context.Background(), *manifestPath, *outputPath); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, manifestPath, outputPath string) error {
	contents, err := os.ReadFile(manifestPath)
	if err != nil {
		return err
	}
	manifest, err := contract.DecodeOperationManifest(contents)
	if err != nil {
		return err
	}
	results, checkErr := contract.CheckOperationSources(ctx, contract.DefaultSourceCheckClient(), manifest)
	report, err := json.MarshalIndent(struct {
		Sources []contract.SourceCheckResult `json:"sources"`
	}{Sources: results}, "", "  ")
	if err != nil {
		return err
	}
	report = append(report, '\n')
	if outputPath == "" {
		if _, err := os.Stdout.Write(report); err != nil {
			return err
		}
	} else if err := os.WriteFile(outputPath, report, 0o600); err != nil {
		return err
	}
	return checkErr
}
