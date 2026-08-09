package release

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDescriptorRejectsNonStableSemVer(t *testing.T) {
	for _, version := range []string{"", "v1.2.3", "1.2", "01.2.3", "1.2.3-rc.1", "1.2.3+build", "1.2.3-SNAPSHOT"} {
		if err := (Descriptor{Version: version}).Validate(); err == nil {
			t.Fatalf("Validate(%q) succeeded", version)
		}
	}
}

func TestDescriptorNext(t *testing.T) {
	base := Descriptor{Version: "1.2.3"}
	for bump, want := range map[string]string{"patch": "1.2.4", "minor": "1.3.0", "major": "2.0.0"} {
		got, err := base.Next(bump)
		if err != nil || got.Version != want {
			t.Fatalf("Next(%q) = %#v, %v; want %q", bump, got, err, want)
		}
	}
}

func TestReadRejectsUnknownAndTrailingFields(t *testing.T) {
	directory := t.TempDir()
	for name, contents := range map[string]string{
		"unknown":  `{"version":"1.2.3","extra":true}`,
		"trailing": `{"version":"1.2.3"}{}`,
	} {
		path := filepath.Join(directory, name+".json")
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Read(path); err == nil {
			t.Fatalf("Read(%s) succeeded", name)
		}
	}
}

func TestWriteRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "version.json")
	want := Descriptor{Version: "0.1.0"}
	if err := Write(path, want); err != nil {
		t.Fatal(err)
	}
	got, err := Read(path)
	if err != nil || got != want {
		t.Fatalf("Read() = %#v, %v", got, err)
	}
}
