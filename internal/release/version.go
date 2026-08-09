// Package release owns the canonical stable product version.
package release

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
)

var stableSemVer = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

type Descriptor struct {
	Version string `json:"version"`
}

func (d Descriptor) Validate() error {
	if !stableSemVer.MatchString(d.Version) {
		return fmt.Errorf("version %q must be a stable MAJOR.MINOR.PATCH SemVer", d.Version)
	}
	return nil
}

func Read(path string) (Descriptor, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return Descriptor{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	var descriptor Descriptor
	if err := decoder.Decode(&descriptor); err != nil {
		return Descriptor{}, fmt.Errorf("decode release descriptor: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return Descriptor{}, errors.New("release descriptor has trailing JSON")
		}
		return Descriptor{}, fmt.Errorf("decode release descriptor: %w", err)
	}
	if err := descriptor.Validate(); err != nil {
		return Descriptor{}, err
	}
	return descriptor, nil
}

func (d Descriptor) Next(bump string) (Descriptor, error) {
	if err := d.Validate(); err != nil {
		return Descriptor{}, err
	}
	matches := stableSemVer.FindStringSubmatch(d.Version)
	major, _ := strconv.ParseUint(matches[1], 10, 64)
	minor, _ := strconv.ParseUint(matches[2], 10, 64)
	patch, _ := strconv.ParseUint(matches[3], 10, 64)
	switch bump {
	case "major":
		major, minor, patch = major+1, 0, 0
	case "minor":
		minor, patch = minor+1, 0
	case "patch":
		patch++
	default:
		return Descriptor{}, fmt.Errorf("unsupported version bump %q", bump)
	}
	return Descriptor{Version: fmt.Sprintf("%d.%d.%d", major, minor, patch)}, nil
}

func Compare(left, right Descriptor) (int, error) {
	if err := left.Validate(); err != nil {
		return 0, err
	}
	if err := right.Validate(); err != nil {
		return 0, err
	}
	leftParts := stableSemVer.FindStringSubmatch(left.Version)
	rightParts := stableSemVer.FindStringSubmatch(right.Version)
	for index := 1; index <= 3; index++ {
		l, _ := strconv.ParseUint(leftParts[index], 10, 64)
		r, _ := strconv.ParseUint(rightParts[index], 10, 64)
		if l < r {
			return -1, nil
		}
		if l > r {
			return 1, nil
		}
	}
	return 0, nil
}

func Write(path string, descriptor Descriptor) error {
	if err := descriptor.Validate(); err != nil {
		return err
	}
	contents, err := json.MarshalIndent(descriptor, "", "  ")
	if err != nil {
		return err
	}
	contents = append(contents, '\n')
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".version-*.json")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer os.Remove(name)
	if _, err := temporary.Write(contents); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Chmod(0o644); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}
