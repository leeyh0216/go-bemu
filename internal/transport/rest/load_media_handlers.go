package rest

// BigQuery's media endpoint separates job metadata from bytes.  The public
// edge never translates media into file://: completed bytes become an opaque
// bqemu-upload:// object and are consumed through the ordinary load planner.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
	loadDomain "github.com/leeyh0216/go-bemu/internal/loadjob/domain"
)

const (
	mediaSessionLimit = 128
	mediaSessionTTL   = 10 * time.Minute
)

type mediaUploadSession struct {
	mu        sync.Mutex
	storeID   string
	payload   []byte
	load      []byte
	total     int64
	created   time.Time
	completed bool
	uri       string
}

type mediaUploadSessions struct {
	mu    sync.Mutex
	items map[string]*mediaUploadSession
}

func (h *combinedJobHandlers) uploadSessions() *mediaUploadSessions {
	return h.sessions
}

func (h *combinedJobHandlers) uploadLoadJob(w http.ResponseWriter, r *http.Request) {
	if h.media == nil {
		writeLoadError(w, fmt.Errorf("%w: media upload is not configured", loadDomain.ErrUnsupported))
		return
	}
	switch r.URL.Query().Get("uploadType") {
	case "multipart":
		if r.Method != http.MethodPost {
			writeLoadError(w, fmt.Errorf("%w: multipart upload requires POST", loadDomain.ErrInvalid))
			return
		}
		h.uploadMultipart(w, r)
	case "resumable":
		if r.Method == http.MethodPost {
			h.startResumable(w, r)
			return
		}
		if r.Method == http.MethodPut {
			h.writeResumable(w, r)
			return
		}
		writeLoadError(w, fmt.Errorf("%w: resumable upload requires POST or PUT", loadDomain.ErrInvalid))
	default:
		writeLoadError(w, fmt.Errorf("%w: uploadType must be multipart or resumable", loadDomain.ErrInvalid))
	}
}

func (h *combinedJobHandlers) uploadMultipart(w http.ResponseWriter, r *http.Request) {
	mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || !strings.EqualFold(mediaType, "multipart/related") || params["boundary"] == "" {
		writeLoadError(w, fmt.Errorf("%w: multipart upload requires multipart/related with boundary", loadDomain.ErrInvalid))
		return
	}
	reader := multipart.NewReader(r.Body, params["boundary"])
	metadata, err := reader.NextPart()
	if err != nil {
		writeLoadError(w, fmt.Errorf("%w: multipart metadata part is required", loadDomain.ErrInvalid))
		return
	}
	if !isJSONMediaType(metadata.Header.Get("Content-Type")) {
		_ = metadata.Close()
		writeLoadError(w, fmt.Errorf("%w: first multipart part must be application/json", loadDomain.ErrInvalid))
		return
	}
	payload, err := io.ReadAll(io.LimitReader(metadata, maximumJSONBodyBytes+1))
	_ = metadata.Close()
	if err != nil || len(payload) > maximumJSONBodyBytes {
		writeLoadError(w, fmt.Errorf("%w: invalid multipart metadata", loadDomain.ErrInvalid))
		return
	}
	loadPayload, err := mediaLoadPayload(payload)
	if err != nil {
		writeLoadError(w, err)
		return
	}
	part, err := reader.NextPart()
	if err != nil {
		writeLoadError(w, fmt.Errorf("%w: multipart media part is required", loadDomain.ErrInvalid))
		return
	}
	id, err := h.media.Create(r.Context())
	if err != nil {
		_ = part.Close()
		writeLoadError(w, err)
		return
	}
	_, writeErr := h.media.Append(r.Context(), id, 0, part)
	_ = part.Close()
	if writeErr != nil {
		_ = h.media.Discard(r.Context(), id)
		writeLoadError(w, writeErr)
		return
	}
	if extra, extraErr := reader.NextPart(); extraErr != io.EOF {
		if extra != nil {
			_ = extra.Close()
		}
		_ = h.media.Discard(r.Context(), id)
		writeLoadError(w, fmt.Errorf("%w: multipart upload must contain exactly metadata and media", loadDomain.ErrInvalid))
		return
	}
	object, err := h.media.Commit(r.Context(), id)
	if err != nil {
		writeLoadError(w, err)
		return
	}
	h.insertLoadJobWithSource(w, r, payload, loadPayload, object.URI)
}

func (h *combinedJobHandlers) startResumable(w http.ResponseWriter, r *http.Request) {
	payload, err := readJSONBody(r)
	if err != nil {
		writeLoadError(w, err)
		return
	}
	loadPayload, err := mediaLoadPayload(payload)
	if err != nil {
		writeLoadError(w, err)
		return
	}
	total, err := resumableTotal(r.Header.Get("X-Upload-Content-Length"))
	if err != nil {
		writeLoadError(w, err)
		return
	}
	manager := h.uploadSessions()
	manager.mu.Lock()
	manager.expireLocked(h.media, time.Now())
	if len(manager.items) >= mediaSessionLimit {
		manager.mu.Unlock()
		writeLoadError(w, fmt.Errorf("%w: too many active media upload sessions", loadDomain.ErrPrecondition))
		return
	}
	manager.mu.Unlock()
	id, err := h.media.Create(r.Context())
	if err != nil {
		writeLoadError(w, err)
		return
	}
	manager.mu.Lock()
	manager.items[id] = &mediaUploadSession{storeID: id, payload: payload, load: loadPayload, total: total, created: time.Now().UTC()}
	manager.mu.Unlock()
	w.Header().Set("Location", "/resumable/upload/bigquery/v2/projects/"+r.PathValue("projectId")+"/jobs?uploadType=resumable&upload_id="+id)
	w.Header().Set("X-GUploader-UploadID", id)
	w.WriteHeader(http.StatusOK)
}

func (h *combinedJobHandlers) writeResumable(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("upload_id")
	if id == "" {
		writeLoadError(w, fmt.Errorf("%w: resumable upload_id is required", loadDomain.ErrInvalid))
		return
	}
	manager := h.uploadSessions()
	manager.mu.Lock()
	manager.expireLocked(h.media, time.Now())
	session := manager.items[id]
	manager.mu.Unlock()
	if session == nil {
		writeLoadError(w, fmt.Errorf("%w: resumable upload session does not exist", loadDomain.ErrNotFound))
		return
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.completed {
		h.insertLoadJobWithSource(w, r, session.payload, session.load, session.uri)
		return
	}
	rangeValue := r.Header.Get("Content-Range")
	if strings.HasPrefix(rangeValue, "bytes */") {
		h.writeResumableStatus(w, r, session)
		return
	}
	start, end, total, err := mediaContentRange(rangeValue)
	if err != nil {
		writeLoadError(w, err)
		return
	}
	if session.total >= 0 && total != session.total {
		writeLoadError(w, fmt.Errorf("%w: resumable total length changed", loadDomain.ErrConflict))
		return
	}
	if session.total < 0 {
		session.total = total
	}
	current, err := h.media.Size(r.Context(), id)
	if err != nil {
		writeLoadError(w, err)
		return
	}
	if start != current {
		h.writeResumableStatus(w, r, session)
		return
	}
	written, err := h.media.Append(r.Context(), id, start, r.Body)
	if err != nil {
		writeLoadError(w, err)
		return
	}
	if written != end-start+1 {
		writeLoadError(w, fmt.Errorf("%w: Content-Range does not match media bytes", loadDomain.ErrInvalid))
		return
	}
	if end+1 < total {
		h.writeResumableStatus(w, r, session)
		return
	}
	if end+1 != total {
		writeLoadError(w, fmt.Errorf("%w: media bytes exceed Content-Range total", loadDomain.ErrInvalid))
		return
	}
	object, err := h.media.Commit(r.Context(), id)
	if err != nil {
		writeLoadError(w, err)
		return
	}
	manager.mu.Lock()
	session.completed, session.uri = true, object.URI
	manager.mu.Unlock()
	h.insertLoadJobWithSource(w, r, session.payload, session.load, object.URI)
}

func (h *combinedJobHandlers) writeResumableStatus(w http.ResponseWriter, r *http.Request, session *mediaUploadSession) {
	if session.completed {
		return
	}
	size, err := h.media.Size(r.Context(), session.storeID)
	if err != nil {
		writeLoadError(w, err)
		return
	}
	if size > 0 {
		w.Header().Set("Range", "bytes=0-"+strconv.FormatInt(size-1, 10))
	}
	w.WriteHeader(http.StatusPermanentRedirect)
}

func (m *mediaUploadSessions) expireLocked(store interface {
	Discard(context.Context, string) error
}, now time.Time) {
	for id, session := range m.items {
		session.mu.Lock()
		expired := !session.completed && now.Sub(session.created) > mediaSessionTTL
		session.mu.Unlock()
		if expired {
			_ = store.Discard(context.Background(), id)
			delete(m.items, id)
		}
	}
}

func mediaLoadPayload(payload []byte) ([]byte, error) {
	var probe combinedJobProbe
	if err := json.Unmarshal(payload, &probe); err != nil {
		return nil, fmt.Errorf("%w: invalid upload job metadata", loadDomain.ErrInvalid)
	}
	if !rawPresent(probe.Configuration.Load) || rawPresent(probe.Configuration.Query) {
		return nil, fmt.Errorf("%w: media upload requires exactly one load configuration", loadDomain.ErrInvalid)
	}
	return probe.Configuration.Load, nil
}
func isJSONMediaType(value string) bool {
	mediaType, _, err := mime.ParseMediaType(value)
	return err == nil && (strings.EqualFold(mediaType, "application/json") || strings.HasSuffix(strings.ToLower(mediaType), "+json"))
}
func resumableTotal(value string) (int64, error) {
	if strings.TrimSpace(value) == "" {
		return -1, nil
	}
	total, err := strconv.ParseInt(value, 10, 64)
	if err != nil || total < 0 {
		return 0, fmt.Errorf("%w: invalid X-Upload-Content-Length", domain.ErrInvalid)
	}
	return total, nil
}
func mediaContentRange(value string) (int64, int64, int64, error) {
	var start, end, total int64
	if _, err := fmt.Sscanf(value, "bytes %d-%d/%d", &start, &end, &total); err != nil || start < 0 || end < start || total <= end {
		return 0, 0, 0, fmt.Errorf("%w: invalid Content-Range", domain.ErrInvalid)
	}
	return start, end, total, nil
}
