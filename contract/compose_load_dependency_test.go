package contract

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestDefaultComposeRequiresFakeGCSForLoadJobs(t *testing.T) {
	root := filepath.Clean("..")
	contents, err := os.ReadFile(filepath.Join(root, "compose.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var compose struct {
		Services map[string]struct {
			Image       string            `yaml:"image"`
			Environment map[string]string `yaml:"environment"`
			DependsOn   map[string]struct {
				Condition string `yaml:"condition"`
			} `yaml:"depends_on"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(contents, &compose); err != nil {
		t.Fatal(err)
	}
	bqemu, ok := compose.Services["bqemu"]
	if !ok {
		t.Fatal("default Compose does not define bqemu")
	}
	fakeGCS, ok := compose.Services["fake-gcs"]
	if !ok || fakeGCS.Image == "" {
		t.Fatal("default Compose does not define a pinned fake-gcs service")
	}
	if bqemu.Environment["BQEMU_LOAD_GCS_ENDPOINT"] != "http://fake-gcs:4443" {
		t.Fatalf("load endpoint = %q", bqemu.Environment["BQEMU_LOAD_GCS_ENDPOINT"])
	}
	if dependency, ok := bqemu.DependsOn["fake-gcs"]; !ok || dependency.Condition != "service_started" {
		t.Fatalf("fake-gcs dependency = %#v", dependency)
	}
	if _, err := os.Stat(filepath.Join(root, "compose.load.yaml")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("optional load overlay still exists: %v", err)
	}
}
