// fixturegen creates checksum-locked Parquet objects for the public load E2E.
// It uses the same pinned DuckDB Go module as the emulator, so no unpinned
// Python Parquet dependency is needed.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

const (
	expectedSchemaVersion = "bqemu-load-e2e-fixtures/v1"
	maxManifestBytes      = 1 << 20
)

type manifest struct {
	SchemaVersion string `json:"schemaVersion"`
	Generator     struct {
		Module  string `json:"module"`
		Version string `json:"version"`
	} `json:"generator"`
	Bucket  string    `json:"bucket"`
	Objects []fixture `json:"objects"`
}

type fixture struct {
	Name   string `json:"name"`
	Query  string `json:"query"`
	Rows   int64  `json:"rows"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

func main() {
	var manifestPath, outputRoot, timeoutValue string
	var observe bool
	flag.StringVar(&manifestPath, "manifest", "tests/load/fixtures.lock.json", "fixture lock path")
	flag.StringVar(&outputRoot, "output-root", "", "seed output root")
	flag.StringVar(&timeoutValue, "timeout", "30s", "overall generation timeout")
	flag.BoolVar(&observe, "observe", false, "emit unlocked sizes and hashes for a lock update")
	flag.Parse()
	if flag.NArg() != 0 || strings.TrimSpace(outputRoot) == "" {
		fatal("arguments", "flag-shape", "set-output-root-and-remove-positional-arguments")
	}
	timeout, err := time.ParseDuration(timeoutValue)
	if err != nil || timeout <= 0 {
		fatal("parse-timeout", "non-positive-or-invalid-duration", "set-a-positive-timeout")
	}
	locked, err := readManifest(manifestPath)
	if err != nil {
		fatal("read-manifest", fingerprint(err.Error()), "repair-fixture-lock")
	}
	if err := validateManifest(locked, observe); err != nil {
		fatal("validate-manifest", fingerprint(err.Error()), "repair-fixture-lock")
	}
	if err := validateDependency(locked.Generator.Module, locked.Generator.Version); err != nil {
		fatal("validate-generator", fingerprint(err.Error()), "update-generator-version-and-fixture-lock")
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := generate(ctx, outputRoot, locked, observe); err != nil {
		fatal("generate-fixtures", fingerprint(err.Error()), "inspect-generator-and-refresh-fixture-lock")
	}
}

func readManifest(path string) (manifest, error) {
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return manifest{}, err
	}
	defer file.Close()
	payload, err := io.ReadAll(io.LimitReader(file, maxManifestBytes+1))
	if err != nil {
		return manifest{}, err
	}
	if len(payload) > maxManifestBytes {
		return manifest{}, errors.New("manifest exceeds byte limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var result manifest
	if err := decoder.Decode(&result); err != nil {
		return manifest{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return manifest{}, errors.New("manifest contains a trailing JSON value")
		}
		return manifest{}, err
	}
	return result, nil
}

func validateManifest(value manifest, observe bool) error {
	if value.SchemaVersion != expectedSchemaVersion || value.Generator.Module == "" || value.Generator.Version == "" {
		return errors.New("manifest identity is incomplete")
	}
	if !safeSegment(value.Bucket) || len(value.Objects) == 0 || len(value.Objects) > 32 {
		return errors.New("bucket or object count is invalid")
	}
	seen := make(map[string]struct{}, len(value.Objects))
	for _, object := range value.Objects {
		cleaned := filepath.ToSlash(filepath.Clean(object.Name))
		if cleaned != object.Name || strings.HasPrefix(cleaned, "/") || strings.HasPrefix(cleaned, "../") || object.Query == "" || object.Rows < 1 {
			return errors.New("fixture shape is invalid")
		}
		for _, segment := range strings.Split(cleaned, "/") {
			if !safeSegment(segment) {
				return errors.New("fixture object name is invalid")
			}
		}
		if _, exists := seen[cleaned]; exists {
			return errors.New("fixture object names are not unique")
		}
		seen[cleaned] = struct{}{}
		if strings.Contains(object.Query, ";") {
			return errors.New("fixture query must be one expression without semicolons")
		}
		if !observe && (object.Bytes < 1 || len(object.SHA256) != sha256.Size*2) {
			return errors.New("fixture checksum lock is incomplete")
		}
		if object.SHA256 != "" {
			if _, err := hex.DecodeString(object.SHA256); err != nil {
				return errors.New("fixture SHA-256 is invalid")
			}
		}
	}
	return nil
}

func validateDependency(module, version string) error {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return errors.New("Go build information is unavailable")
	}
	for _, dependency := range info.Deps {
		if dependency.Path == module {
			actual := dependency.Version
			if dependency.Replace != nil {
				actual = dependency.Replace.Version
			}
			if actual != version {
				return fmt.Errorf("generator dependency version mismatch")
			}
			return nil
		}
	}
	return errors.New("generator dependency is absent")
}

func generate(ctx context.Context, outputRoot string, locked manifest, observe bool) error {
	database, err := sql.Open("duckdb", "")
	if err != nil {
		return err
	}
	defer database.Close()
	root, err := filepath.Abs(outputRoot)
	if err != nil {
		return err
	}
	for _, object := range locked.Objects {
		if err := ctx.Err(); err != nil {
			return err
		}
		path := filepath.Join(root, locked.Bucket, filepath.FromSlash(object.Name))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return err
		}
		quotedPath := "'" + strings.ReplaceAll(path, "'", "''") + "'"
		if _, err := database.ExecContext(ctx, "COPY ("+object.Query+") TO "+quotedPath+" (FORMAT PARQUET)"); err != nil {
			return err
		}
		payload, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		actualSHA := rawDigest(payload)
		if !observe && (int64(len(payload)) != object.Bytes || actualSHA != object.SHA256) {
			return errors.New("generated fixture differs from checksum lock")
		}
		fmt.Printf("{\"boundary\":\"fixture\",\"bytes\":%d,\"object\":%q,\"rows\":%d,\"sha256\":%q}\n", len(payload), object.Name, object.Rows, actualSHA)
	}
	return nil
}

func safeSegment(value string) bool {
	if value == "" || value == "." || value == ".." {
		return false
	}
	for _, character := range value {
		if (character >= 'a' && character <= 'z') || (character >= '0' && character <= '9') || character == '-' || character == '_' || character == '.' {
			continue
		}
		return false
	}
	return true
}

func rawDigest(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func fingerprint(value string) string {
	return "sha256:" + rawDigest([]byte(value))
}

func fatal(operation, shape, fixHint string) {
	fmt.Fprintf(os.Stderr, "stage=fixture-generator service=duckdb model_version=v2.10505.0 operation=%s shape=%s fix_hint=%s\n", operation, shape, fixHint)
	os.Exit(1)
}
