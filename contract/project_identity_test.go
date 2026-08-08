package contract

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// Project-owned names use BQEMU/BEMU. Protocol-defined BigQuery names and
// upstream repository URLs remain untouched because they are compatibility
// contracts. This guard therefore rejects only known legacy project-owned
// identifiers in Git-tracked files.
func TestProjectOwnedIdentityDoesNotRegress(t *testing.T) {
	root := filepath.Clean("..")
	files := trackedProjectFiles(t, root)
	forbidden := []struct {
		name  string
		value []byte
	}{
		{name: "legacy repository slug", value: []byte("go-" + "bigquery-emulator")},
		{name: "legacy module path", value: []byte("github.com/leeyh0216/" + "go-" + "bigquery-emulator")},
		{name: "legacy environment prefix", value: []byte("GO_" + "BIGQUERY_EMULATOR_")},
	}

	for _, relative := range files {
		contents, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(relative)))
		if err != nil {
			t.Fatalf("read tracked file %s: %v", relative, err)
		}
		if bytes.IndexByte(contents, 0) >= 0 {
			continue
		}
		for _, legacy := range forbidden {
			if bytes.Contains(contents, legacy.value) {
				t.Errorf("%s reintroduces %s; use project-owned BQEMU/BEMU names", relative, legacy.name)
			}
		}
	}
}

func TestProductSourceDoesNotOwnIntegrationProfiles(t *testing.T) {
	root := filepath.Clean("..")
	productPrefixes := []string{"cmd/", "configs/", "contract/", "internal/"}
	forbidden := []*regexp.Regexp{
		regexp.MustCompile(`(?i)\b` + "sp" + `ark\b`),
		regexp.MustCompile(`(?i)\b` + "py" + "sp" + `ark\b`),
		regexp.MustCompile(`(?i)\b` + "b" + `q\b`),
		regexp.MustCompile("v0" + "442"),
		regexp.MustCompile(regexp.QuoteMeta("0." + "44.2")),
		regexp.MustCompile(regexp.QuoteMeta("2." + "1.31")),
		regexp.MustCompile(regexp.QuoteMeta("3." + "5.8")),
		regexp.MustCompile(regexp.QuoteMeta("tests/" + "integration")),
	}
	for _, relative := range trackedProjectFiles(t, root) {
		if !hasAnyPrefix(relative, productPrefixes) {
			continue
		}
		contents, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(relative)))
		if err != nil {
			t.Fatalf("read product source %s: %v", relative, err)
		}
		if bytes.IndexByte(contents, 0) >= 0 {
			continue
		}
		for _, pattern := range forbidden {
			if pattern.Match(contents) {
				t.Errorf("%s contains integration-profile content matched by %q", relative, pattern.String())
			}
		}
	}
}

func TestProjectIdentityEntrypointsRemainPinned(t *testing.T) {
	root := filepath.Clean("..")
	required := map[string][]string{
		"go.mod": {
			"module github.com/leeyh0216/go-bemu\n",
		},
		"Dockerfile": {
			`org.opencontainers.image.source="https://github.com/leeyh0216/go-bemu"`,
			`ENTRYPOINT ["/usr/local/bin/go-bemu"]`,
		},
		"compose.yaml": {
			"name: go-bemu\n",
			"  bqemu:\n",
		},
		"Makefile": {
			"BINARY ?= bin/go-bemu\n",
			"IMAGE ?= go-bemu:dev\n",
		},
		"README.md": {
			"# go-bemu\n",
		},
		"README.ko.md": {
			"# go-bemu\n",
		},
	}

	for relative, markers := range required {
		contents, err := os.ReadFile(filepath.Join(root, relative))
		if err != nil {
			t.Fatalf("read identity entrypoint %s: %v", relative, err)
		}
		for _, marker := range markers {
			if !bytes.Contains(contents, []byte(marker)) {
				t.Errorf("%s is missing project identity marker %q", relative, marker)
			}
		}
	}
}

func trackedProjectFiles(t *testing.T, root string) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "git", "ls-files", "-z")
	command.Dir = root
	output, err := command.Output()
	if err != nil {
		t.Skipf("project identity guard requires a Git worktree: %v", err)
	}
	if err := ctx.Err(); err != nil {
		t.Fatalf("list tracked project files: %v", err)
	}

	parts := strings.Split(string(output), "\x00")
	files := make([]string, 0, len(parts))
	for _, relative := range parts {
		if relative != "" {
			files = append(files, filepath.ToSlash(relative))
		}
	}
	if len(files) == 0 {
		t.Fatal("project identity guard found no tracked files")
	}
	return files
}

func hasAnyPrefix(value string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(value, prefix) {
			return true
		}
	}
	return false
}
