package objectstore

// The media store is deliberately content-addressed.  A completed upload has
// no mutable pathname visible to a client and survives the hand-off from the
// HTTP resumable-session state to an asynchronous load job.

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/leeyh0216/go-bemu/internal/loadjob/domain"
	loadports "github.com/leeyh0216/go-bemu/internal/loadjob/ports"
)

const mediaScheme = "bqemu-upload"

type MediaStore struct {
	root     string
	maxBytes int64
	mu       sync.Mutex
	staging  map[string]string
}

func NewMediaStore(root string, maxBytes int64) (*MediaStore, error) {
	if strings.TrimSpace(root) == "" || maxBytes <= 0 {
		return nil, fmt.Errorf("%w: media root and maximum bytes are required", domain.ErrInvalid)
	}
	root = filepath.Clean(root)
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("create media store: %w", err)
	}
	if err := os.Chmod(root, 0o700); err != nil {
		return nil, fmt.Errorf("secure media store: %w", err)
	}
	// Incomplete sessions are intentionally not resumed after a process restart:
	// their in-memory protocol metadata is gone.  Remove only our private
	// staging files, never arbitrary files in the configured directory.
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, fmt.Errorf("inspect media store: %w", err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".upload-") && strings.HasSuffix(entry.Name(), ".part") {
			if err := os.Remove(filepath.Join(root, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("clean incomplete media upload: %w", err)
			}
		}
	}
	return &MediaStore{root: root, maxBytes: maxBytes, staging: make(map[string]string)}, nil
}

func (s *MediaStore) Create(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate media upload ID: %w", err)
	}
	id := hex.EncodeToString(raw[:])
	path := filepath.Join(s.root, ".upload-"+id+".part")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return "", fmt.Errorf("create media staging object: %w", err)
	}
	if err := file.Close(); err != nil {
		return "", fmt.Errorf("close media staging object: %w", err)
	}
	s.mu.Lock()
	s.staging[id] = path
	s.mu.Unlock()
	return id, nil
}

func (s *MediaStore) Append(ctx context.Context, id string, offset int64, payload io.Reader) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	path, err := s.stagingPath(id)
	if err != nil {
		return 0, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return 0, mediaFileError(err)
	}
	if offset != info.Size() {
		return 0, fmt.Errorf("%w: upload chunk offset does not match committed bytes", domain.ErrConflict)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return 0, mediaFileError(err)
	}
	defer file.Close()
	remaining := s.maxBytes - offset
	if remaining < 0 {
		return 0, fmt.Errorf("%w: upload exceeds configured size limit", domain.ErrPrecondition)
	}
	written, copyErr := io.Copy(file, io.LimitReader(payload, remaining+1))
	if copyErr != nil {
		return written, fmt.Errorf("write media upload: %w", copyErr)
	}
	if written > remaining {
		_ = os.Truncate(path, offset)
		return 0, fmt.Errorf("%w: upload exceeds configured size limit", domain.ErrPrecondition)
	}
	return written, nil
}

func (s *MediaStore) Size(ctx context.Context, id string) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	path, err := s.stagingPath(id)
	if err != nil {
		return 0, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return 0, mediaFileError(err)
	}
	return info.Size(), nil
}

func (s *MediaStore) Commit(ctx context.Context, id string) (loadports.ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return loadports.ObjectInfo{}, err
	}
	path, err := s.stagingPath(id)
	if err != nil {
		return loadports.ObjectInfo{}, err
	}
	file, err := os.Open(path)
	if err != nil {
		return loadports.ObjectInfo{}, mediaFileError(err)
	}
	hash := sha256.New()
	size, copyErr := io.Copy(hash, file)
	closeErr := file.Close()
	if copyErr != nil {
		return loadports.ObjectInfo{}, fmt.Errorf("hash media upload: %w", copyErr)
	}
	if closeErr != nil {
		return loadports.ObjectInfo{}, fmt.Errorf("close media upload: %w", closeErr)
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	target := filepath.Join(s.root, digest+".object")
	if err := os.Rename(path, target); err != nil && !errors.Is(err, os.ErrExist) {
		return loadports.ObjectInfo{}, fmt.Errorf("publish immutable media upload: %w", err)
	}
	s.mu.Lock()
	delete(s.staging, id)
	s.mu.Unlock()
	return loadports.ObjectInfo{URI: mediaURI(digest), Size: size, Generation: digest, ETag: digest}, nil
}

func (s *MediaStore) Discard(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	path, ok := s.staging[id]
	delete(s.staging, id)
	s.mu.Unlock()
	if !ok {
		return fmt.Errorf("%w: upload session does not exist", domain.ErrNotFound)
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("discard media upload: %w", err)
	}
	return nil
}

func (s *MediaStore) Get(ctx context.Context, rawURI string) (loadports.ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return loadports.ObjectInfo{}, err
	}
	digest, err := mediaDigest(rawURI)
	if err != nil {
		return loadports.ObjectInfo{}, err
	}
	info, err := os.Stat(filepath.Join(s.root, digest+".object"))
	if err != nil {
		return loadports.ObjectInfo{}, mediaFileError(err)
	}
	if !info.Mode().IsRegular() {
		return loadports.ObjectInfo{}, fmt.Errorf("%w: media object is not regular", domain.ErrInvalid)
	}
	return loadports.ObjectInfo{URI: mediaURI(digest), Size: info.Size(), Generation: digest, ETag: digest}, nil
}
func (s *MediaStore) List(context.Context, string) ([]loadports.ObjectInfo, error) {
	return nil, fmt.Errorf("%w: media upload objects do not support wildcards", domain.ErrUnsupported)
}
func (s *MediaStore) Open(ctx context.Context, object loadports.ObjectInfo) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	digest, err := mediaDigest(object.URI)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(filepath.Join(s.root, digest+".object"))
	if err != nil {
		return nil, mediaFileError(err)
	}
	return file, nil
}
func (s *MediaStore) stagingPath(id string) (string, error) {
	s.mu.Lock()
	path, ok := s.staging[id]
	s.mu.Unlock()
	if !ok {
		return "", fmt.Errorf("%w: upload session does not exist", domain.ErrNotFound)
	}
	return path, nil
}
func mediaURI(digest string) string {
	return (&url.URL{Scheme: mediaScheme, Host: "media", Path: "/" + digest}).String()
}
func mediaDigest(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != mediaScheme || u.Host != "media" || strings.TrimPrefix(u.Path, "/") == "" || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("%w: invalid media upload URI", domain.ErrInvalid)
	}
	digest := strings.TrimPrefix(u.Path, "/")
	if len(digest) != 64 {
		return "", fmt.Errorf("%w: invalid media upload URI", domain.ErrInvalid)
	}
	if _, err := hex.DecodeString(digest); err != nil {
		return "", fmt.Errorf("%w: invalid media upload URI", domain.ErrInvalid)
	}
	return digest, nil
}
func mediaFileError(err error) error {
	if errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%w: media upload object does not exist", domain.ErrNotFound)
	}
	return fmt.Errorf("access media upload object: %w", err)
}

var _ loadports.ObjectStore = (*MediaStore)(nil)
var _ loadports.MediaUploadStore = (*MediaStore)(nil)
