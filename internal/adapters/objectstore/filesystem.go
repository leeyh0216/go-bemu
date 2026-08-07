package objectstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/leeyh0216/go-bemu/internal/loadjob/domain"
	loadports "github.com/leeyh0216/go-bemu/internal/loadjob/ports"
)

// FileSystem serves explicit file:// URIs. Bare host paths are intentionally
// rejected so a malformed gs:// URI cannot silently become a local read.
type FileSystem struct{}

func (FileSystem) Get(ctx context.Context, rawURI string) (loadports.ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return loadports.ObjectInfo{}, err
	}
	path, err := filePath(rawURI)
	if err != nil {
		return loadports.ObjectInfo{}, err
	}
	if strings.ContainsAny(path, "*?[") {
		return loadports.ObjectInfo{}, fmt.Errorf("%w: exact object URI contains a glob", domain.ErrInvalid)
	}
	info, err := os.Stat(path)
	if err != nil {
		return loadports.ObjectInfo{}, fileSystemError(err)
	}
	if !info.Mode().IsRegular() {
		return loadports.ObjectInfo{}, fmt.Errorf("%w: file object is not regular", domain.ErrInvalid)
	}
	return loadports.ObjectInfo{URI: canonicalFileURI(path), Size: info.Size()}, nil
}

func (FileSystem) List(ctx context.Context, rawPattern string) ([]loadports.ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	pattern, err := filePath(rawPattern)
	if err != nil {
		return nil, err
	}
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid file URI glob", domain.ErrInvalid)
	}
	sort.Strings(matches)
	objects := make([]loadports.ObjectInfo, 0, len(matches))
	for _, match := range matches {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		info, err := os.Stat(match)
		if err != nil {
			return nil, fileSystemError(err)
		}
		if !info.Mode().IsRegular() {
			continue
		}
		objects = append(objects, loadports.ObjectInfo{URI: canonicalFileURI(match), Size: info.Size()})
	}
	return objects, nil
}

func (FileSystem) Open(ctx context.Context, object loadports.ObjectInfo) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path, err := filePath(object.URI)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fileSystemError(err)
	}
	return file, nil
}

func filePath(rawURI string) (string, error) {
	u, err := url.Parse(rawURI)
	if err != nil {
		return "", fmt.Errorf("%w: malformed file URI", domain.ErrInvalid)
	}
	if u.Scheme != "file" || (u.Host != "" && !strings.EqualFold(u.Host, "localhost")) || u.Opaque != "" || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("%w: object URI must be a local file:// URI", domain.ErrInvalid)
	}
	if u.Path == "" {
		return "", fmt.Errorf("%w: file URI path is required", domain.ErrInvalid)
	}
	return filepath.FromSlash(u.Path), nil
}

func canonicalFileURI(path string) string {
	absolute, err := filepath.Abs(path)
	if err == nil {
		path = absolute
	}
	return (&url.URL{Scheme: "file", Path: filepath.ToSlash(path)}).String()
}

func fileSystemError(err error) error {
	if errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%w: file object does not exist", domain.ErrNotFound)
	}
	if errors.Is(err, os.ErrPermission) {
		return fmt.Errorf("%w: file object is not readable", domain.ErrPrecondition)
	}
	return fmt.Errorf("access file object: %w", err)
}

var _ loadports.ObjectStore = FileSystem{}
