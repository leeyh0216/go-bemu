package rest

// BigQuery media upload protocol:
// https://cloud.google.com/bigquery/docs/reference/api-uploads

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
	loadDomain "github.com/leeyh0216/go-bemu/internal/loadjob/domain"
	loadports "github.com/leeyh0216/go-bemu/internal/loadjob/ports"
)

const mediaUploadMetadataLimit = 2 << 20

// MediaUploadConfig bounds process-local resumable state. Completed media is
// persisted to the required GCS-compatible service before a normal load job is
// submitted, so public load jobs still have only immutable gs:// sources.
type MediaUploadConfig struct {
	Bucket        string
	MaxSessions   int
	MaxBytes      int64
	MaxChunkBytes int64
	SessionTTL    time.Duration
}

func (c MediaUploadConfig) validate() error {
	if strings.TrimSpace(c.Bucket) == "" || c.MaxSessions < 1 || c.MaxBytes < 1 || c.MaxChunkBytes < 1 || c.SessionTTL <= 0 {
		return fmt.Errorf("%w: media upload configuration is invalid", domain.ErrInvalid)
	}
	if c.MaxChunkBytes > c.MaxBytes {
		return fmt.Errorf("%w: media upload maxChunkBytes must not exceed maxBytes", domain.ErrInvalid)
	}
	return nil
}

type MediaUploadSupport struct {
	store  loadports.MediaObjectWriter
	config MediaUploadConfig
	now    func() time.Time
	newID  func() string
}

func NewMediaUploadSupport(store loadports.MediaObjectWriter, config MediaUploadConfig) (*MediaUploadSupport, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: media upload object store is required", domain.ErrInvalid)
	}
	if err := config.validate(); err != nil {
		return nil, err
	}
	return &MediaUploadSupport{
		store: store, config: config, now: func() time.Time { return time.Now().UTC() }, newID: newMediaUploadID,
	}, nil
}

func newMediaUploadID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err == nil {
		return hex.EncodeToString(raw[:])
	}
	return strconv.FormatInt(time.Now().UnixNano(), 36)
}

// WithMediaUpload registers the standard BigQuery media-upload endpoints for
// jobs.insert. The feature is always backed by the configured GCS endpoint.
func WithMediaUpload(support *MediaUploadSupport) Option {
	return func(server *Server) {
		server.mediaUpload = support
	}
}

type mediaUploadHandlers struct {
	jobs    *combinedJobHandlers
	support *MediaUploadSupport
	baseURL string

	mu       sync.Mutex
	sessions map[string]*mediaUploadSession
	used     int64
	reserved int64
}

type mediaUploadSession struct {
	id          string
	projectID   string
	submission  loadSubmission
	contentType string
	expected    int64 // -1 while the client has not declared a total size.
	payload     []byte
	createdAt   time.Time
	updatedAt   time.Time
	finalizing  bool
	completed   *loadDomain.Job
}

func newMediaUploadHandlers(jobs *combinedJobHandlers, support *MediaUploadSupport, baseURL string) *mediaUploadHandlers {
	return &mediaUploadHandlers{
		jobs: jobs, support: support, baseURL: strings.TrimRight(baseURL, "/"), sessions: make(map[string]*mediaUploadSession),
	}
}

func (h *mediaUploadHandlers) start(w http.ResponseWriter, r *http.Request) {
	switch strings.ToLower(strings.TrimSpace(r.URL.Query().Get("uploadType"))) {
	case "multipart":
		h.multipart(w, r)
	case "resumable":
		h.beginResumable(w, r)
	default:
		writeError(w, mediaProtocolError(http.StatusBadRequest, "invalid", "uploadType must be multipart or resumable", domain.ErrInvalid))
	}
}

func (h *mediaUploadHandlers) multipart(w http.ResponseWriter, r *http.Request) {
	mediaType, parameters, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || !strings.EqualFold(mediaType, "multipart/related") || strings.TrimSpace(parameters["boundary"]) == "" {
		writeError(w, mediaProtocolError(http.StatusBadRequest, "invalid", "multipart upload requires Content-Type multipart/related with a boundary", domain.ErrInvalid))
		return
	}
	reservation, err := h.reserveMultipart()
	if err != nil {
		writeError(w, err)
		return
	}
	defer h.releaseReservation(reservation)
	reader := multipart.NewReader(r.Body, parameters["boundary"])
	metadataPart, err := reader.NextPart()
	if err != nil {
		writeError(w, mediaProtocolError(http.StatusBadRequest, "invalid", "multipart upload requires metadata first", domain.ErrInvalid))
		return
	}
	metadata, err := readMediaPart(metadataPart, mediaUploadMetadataLimit)
	_ = metadataPart.Close()
	if err != nil {
		writeError(w, err)
		return
	}
	if !isJSONMediaType(metadataPart.Header.Get("Content-Type")) {
		writeError(w, mediaProtocolError(http.StatusBadRequest, "invalid", "multipart metadata must be application/json", domain.ErrInvalid))
		return
	}
	mediaPart, err := reader.NextPart()
	if err != nil {
		writeError(w, mediaProtocolError(http.StatusBadRequest, "invalid", "multipart upload requires exactly one media part", domain.ErrInvalid))
		return
	}
	contentType, err := mediaContentType(mediaPart.Header.Get("Content-Type"))
	if err != nil {
		_ = mediaPart.Close()
		writeError(w, err)
		return
	}
	payload, err := readMediaPart(mediaPart, reservation)
	_ = mediaPart.Close()
	if err != nil {
		writeError(w, err)
		return
	}
	if extra, extraErr := reader.NextPart(); extraErr != io.EOF {
		if extra != nil {
			_ = extra.Close()
		}
		writeError(w, mediaProtocolError(http.StatusBadRequest, "invalid", "multipart upload must contain exactly two parts", domain.ErrInvalid))
		return
	}
	id := h.support.newID()
	submission, err := h.prepareMediaSubmission(r.PathValue("projectId"), metadata, id)
	if err != nil {
		writeLoadError(w, err)
		return
	}
	job, err := h.complete(r.Context(), id, contentType, payload, submission)
	if err != nil {
		writeLoadError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, loadJobFromDomain(job))
}

func (h *mediaUploadHandlers) beginResumable(w http.ResponseWriter, r *http.Request) {
	if !isJSONMediaType(r.Header.Get("Content-Type")) {
		writeError(w, mediaProtocolError(http.StatusBadRequest, "invalid", "resumable initiation metadata must be application/json", domain.ErrInvalid))
		return
	}
	metadata, err := readMediaMetadata(r.Body)
	if err != nil {
		writeError(w, err)
		return
	}
	id := h.support.newID()
	submission, err := h.prepareMediaSubmission(r.PathValue("projectId"), metadata, id)
	if err != nil {
		writeLoadError(w, err)
		return
	}
	expected, err := declaredUploadLength(r.Header.Get("X-Upload-Content-Length"), h.support.config.MaxBytes)
	if err != nil {
		writeError(w, err)
		return
	}
	contentType, err := mediaContentType(r.Header.Get("X-Upload-Content-Type"))
	if err != nil {
		writeError(w, err)
		return
	}
	now := h.support.now()
	h.mu.Lock()
	h.pruneLocked(now)
	h.trimCompletedLocked()
	if h.activeSessionsLocked() >= h.support.config.MaxSessions {
		h.mu.Unlock()
		writeError(w, mediaProtocolError(http.StatusTooManyRequests, "rateLimitExceeded", "too many resumable upload sessions", domain.ErrPrecondition))
		return
	}
	h.sessions[id] = &mediaUploadSession{
		id: id, projectID: r.PathValue("projectId"), submission: submission, contentType: contentType,
		expected: expected, createdAt: now, updatedAt: now,
	}
	h.mu.Unlock()
	w.Header().Set("Location", h.location(r, id))
	w.WriteHeader(http.StatusOK)
}

func (h *mediaUploadHandlers) resume(w http.ResponseWriter, r *http.Request) {
	if !strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("uploadType")), "resumable") {
		writeError(w, mediaProtocolError(http.StatusBadRequest, "invalid", "resumable session requires uploadType=resumable", domain.ErrInvalid))
		return
	}
	id := strings.TrimSpace(r.URL.Query().Get("upload_id"))
	if id == "" {
		writeError(w, mediaProtocolError(http.StatusBadRequest, "invalid", "resumable session requires upload_id", domain.ErrInvalid))
		return
	}
	rangeSpec, err := parseUploadContentRange(r.Header.Get("Content-Range"))
	if err != nil {
		writeError(w, err)
		return
	}
	if rangeSpec.probe {
		h.probe(r.Context(), w, id, rangeSpec)
		return
	}
	chunkBytes, err := rangeSpec.length()
	if err != nil {
		writeError(w, err)
		return
	}
	if chunkBytes > h.support.config.MaxChunkBytes {
		writeError(w, requestBodyTooLarge(fmt.Errorf("resumable upload chunk exceeds %d bytes", h.support.config.MaxChunkBytes)))
		return
	}
	if rangeSpec.totalKnown && rangeSpec.total > h.support.config.MaxBytes {
		writeError(w, requestBodyTooLarge(fmt.Errorf("resumable upload exceeds %d bytes", h.support.config.MaxBytes)))
		return
	}
	if err := h.reserveBytes(chunkBytes); err != nil {
		writeError(w, err)
		return
	}
	defer h.releaseReservation(chunkBytes)
	payload, err := readMediaPart(r.Body, chunkBytes)
	if err != nil {
		writeError(w, err)
		return
	}
	if int64(len(payload)) != rangeSpec.end-rangeSpec.start+1 {
		writeError(w, mediaProtocolError(http.StatusBadRequest, "invalid", "Content-Range does not match request body length", domain.ErrInvalid))
		return
	}
	job, received, err := h.append(r.Context(), id, rangeSpec, payload, chunkBytes)
	if err != nil {
		writeMediaLoadError(w, err)
		return
	}
	if job != nil {
		writeJSON(w, http.StatusOK, loadJobFromDomain(job))
		return
	}
	writeResumeIncomplete(w, received)
}

func (h *mediaUploadHandlers) probe(ctx context.Context, w http.ResponseWriter, id string, rangeSpec uploadContentRange) {
	h.mu.Lock()
	h.pruneLocked(h.support.now())
	session, ok := h.sessions[id]
	if !ok {
		h.mu.Unlock()
		writeError(w, mediaProtocolError(http.StatusNotFound, "notFound", "resumable upload session was not found", domain.ErrNotFound))
		return
	}
	if session.completed != nil {
		job := session.completed.Clone()
		h.mu.Unlock()
		writeJSON(w, http.StatusOK, loadJobFromDomain(job))
		return
	}
	if session.finalizing {
		h.mu.Unlock()
		writeError(w, mediaProtocolError(http.StatusConflict, "duplicate", "resumable upload is finalizing", domain.ErrConflict))
		return
	}
	if rangeSpec.totalKnown && session.expected >= 0 && session.expected != rangeSpec.total {
		h.mu.Unlock()
		writeError(w, mediaProtocolError(http.StatusBadRequest, "invalid", "resumable status probe total does not match session", domain.ErrInvalid))
		return
	}
	if rangeSpec.totalKnown {
		if rangeSpec.total > h.support.config.MaxBytes || int64(len(session.payload)) > rangeSpec.total {
			h.mu.Unlock()
			writeError(w, mediaProtocolError(http.StatusBadRequest, "invalid", "resumable status probe total is invalid", domain.ErrInvalid))
			return
		}
		session.expected = rangeSpec.total
	}
	received := int64(len(session.payload))
	if session.expected < 0 || received < session.expected {
		h.mu.Unlock()
		writeResumeIncomplete(w, received)
		return
	}
	if received > session.expected {
		h.mu.Unlock()
		writeError(w, mediaProtocolError(http.StatusBadRequest, "invalid", "resumable upload exceeds declared total", domain.ErrInvalid))
		return
	}
	session.finalizing = true
	session.updatedAt = h.support.now()
	h.mu.Unlock()

	job, err := h.finalize(ctx, session)
	if err != nil {
		writeMediaLoadError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, loadJobFromDomain(job))
}

func (h *mediaUploadHandlers) append(ctx context.Context, id string, rangeSpec uploadContentRange, payload []byte, reservation int64) (*loadDomain.Job, int64, error) {
	h.mu.Lock()
	h.pruneLocked(h.support.now())
	session, ok := h.sessions[id]
	if !ok {
		h.mu.Unlock()
		return nil, 0, mediaProtocolError(http.StatusNotFound, "notFound", "resumable upload session was not found", domain.ErrNotFound)
	}
	if session.completed != nil {
		job := session.completed.Clone()
		h.mu.Unlock()
		return job, 0, nil
	}
	if session.finalizing {
		h.mu.Unlock()
		return nil, 0, mediaProtocolError(http.StatusConflict, "duplicate", "resumable upload is finalizing", domain.ErrConflict)
	}
	if rangeSpec.totalKnown && session.expected >= 0 && session.expected != rangeSpec.total {
		h.mu.Unlock()
		return nil, 0, mediaProtocolError(http.StatusBadRequest, "invalid", "Content-Range total does not match session", domain.ErrInvalid)
	}
	if rangeSpec.totalKnown && rangeSpec.total > h.support.config.MaxBytes {
		h.mu.Unlock()
		return nil, 0, requestBodyTooLarge(fmt.Errorf("resumable upload exceeds %d bytes", h.support.config.MaxBytes))
	}
	received := int64(len(session.payload))
	if rangeSpec.start < received {
		if rangeSpec.end >= received || !bytes.Equal(payload, session.payload[rangeSpec.start:rangeSpec.end+1]) {
			h.mu.Unlock()
			return nil, 0, mediaProtocolError(http.StatusBadRequest, "invalid", "resumable upload range overlaps different bytes", domain.ErrInvalid)
		}
		if rangeSpec.totalKnown && received > rangeSpec.total {
			h.mu.Unlock()
			return nil, 0, mediaProtocolError(http.StatusBadRequest, "invalid", "resumable upload exceeds declared total", domain.ErrInvalid)
		}
		if rangeSpec.totalKnown && session.expected < 0 {
			session.expected = rangeSpec.total
		}
		if session.expected < 0 || received < session.expected {
			h.mu.Unlock()
			return nil, received, nil
		}
	} else if rangeSpec.start != received {
		h.mu.Unlock()
		return nil, 0, mediaProtocolError(http.StatusBadRequest, "invalid", "resumable upload range has a gap", domain.ErrInvalid)
	} else {
		if (session.expected >= 0 && received+int64(len(payload)) > session.expected) ||
			(rangeSpec.totalKnown && received+int64(len(payload)) > rangeSpec.total) ||
			h.used+h.reserved-reservation+int64(len(payload)) > h.support.config.MaxBytes {
			h.mu.Unlock()
			return nil, 0, requestBodyTooLarge(fmt.Errorf("resumable upload exceeds configured byte limit"))
		}
		session.payload = append(session.payload, payload...)
		h.used += int64(len(payload))
		received = int64(len(session.payload))
	}
	if rangeSpec.totalKnown && session.expected < 0 {
		session.expected = rangeSpec.total
	}
	session.updatedAt = h.support.now()
	if session.expected < 0 || received < session.expected {
		h.mu.Unlock()
		return nil, received, nil
	}
	if received > session.expected {
		h.mu.Unlock()
		return nil, 0, mediaProtocolError(http.StatusBadRequest, "invalid", "resumable upload exceeds declared total", domain.ErrInvalid)
	}
	session.finalizing = true
	h.mu.Unlock()

	job, err := h.finalize(ctx, session)
	if err != nil {
		return nil, 0, err
	}
	return job, received, nil
}

// finalize writes the session's immutable payload without copying it. A
// finalizing session cannot accept more chunks or be pruned, so the slice is
// stable until this method records either a completed job or a retryable
// failure.
func (h *mediaUploadHandlers) finalize(ctx context.Context, session *mediaUploadSession) (*loadDomain.Job, error) {
	job, err := h.complete(ctx, session.id, session.contentType, session.payload, session.submission)
	if err != nil {
		h.mu.Lock()
		if current := h.sessions[session.id]; current == session {
			current.finalizing = false
		}
		h.mu.Unlock()
		return nil, err
	}
	h.mu.Lock()
	if current := h.sessions[session.id]; current == session {
		current.completed = job.Clone()
		current.finalizing = false
		h.used -= int64(len(current.payload))
		current.payload = nil
		current.updatedAt = h.support.now()
	}
	h.mu.Unlock()
	return job, nil
}

func (h *mediaUploadHandlers) complete(ctx context.Context, id, contentType string, payload []byte, submission loadSubmission) (*loadDomain.Job, error) {
	object, err := h.support.store.Upload(ctx, h.support.config.Bucket, h.objectName(submission.reference.ProjectID, id), contentType, bytes.NewReader(payload), int64(len(payload)))
	if err != nil {
		return nil, err
	}
	job, err := h.jobs.submitLoad(ctx, submission)
	if err == nil {
		return job, nil
	}
	if deleteErr := h.support.store.Delete(context.WithoutCancel(ctx), object); deleteErr != nil {
		return nil, fmt.Errorf("submit uploaded load job: %w; delete uploaded object: %v", err, deleteErr)
	}
	return nil, err
}

func (h *mediaUploadHandlers) prepareMediaSubmission(projectID string, metadata []byte, id string) (loadSubmission, error) {
	sourceURI := "gs://" + h.support.config.Bucket + "/" + h.objectName(projectID, id)
	payload, loadPayload, err := injectMediaSource(metadata, sourceURI)
	if err != nil {
		return loadSubmission{}, err
	}
	submission, err := decodeLoadSubmission(projectID, payload, loadPayload)
	if err != nil {
		return loadSubmission{}, err
	}
	if !strings.EqualFold(string(submission.configuration.SourceFormat), string(loadDomain.FormatParquet)) {
		return loadSubmission{}, fmt.Errorf("%w: media upload supports only PARQUET sourceFormat", loadDomain.ErrUnsupported)
	}
	return submission, nil
}

func injectMediaSource(metadata []byte, sourceURI string) ([]byte, []byte, error) {
	var probe combinedJobProbe
	if err := json.Unmarshal(metadata, &probe); err != nil {
		return nil, nil, fmt.Errorf("%w: invalid load job JSON", loadDomain.ErrInvalid)
	}
	if rawPresent(probe.Configuration.Query) || !rawPresent(probe.Configuration.Load) {
		return nil, nil, fmt.Errorf("%w: media upload metadata must contain exactly one load configuration", loadDomain.ErrInvalid)
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(metadata, &root); err != nil {
		return nil, nil, fmt.Errorf("%w: media upload metadata must be an object", loadDomain.ErrInvalid)
	}
	var configuration map[string]json.RawMessage
	if err := json.Unmarshal(root["configuration"], &configuration); err != nil {
		return nil, nil, fmt.Errorf("%w: media upload configuration must be an object", loadDomain.ErrInvalid)
	}
	var load map[string]json.RawMessage
	if err := json.Unmarshal(configuration["load"], &load); err != nil {
		return nil, nil, fmt.Errorf("%w: media upload load configuration must be an object", loadDomain.ErrInvalid)
	}
	if _, present := load["sourceUris"]; present {
		return nil, nil, fmt.Errorf("%w: media upload metadata must not specify sourceUris", loadDomain.ErrInvalid)
	}
	sources, err := json.Marshal([]string{sourceURI})
	if err != nil {
		return nil, nil, fmt.Errorf("encode media upload source URI: %w", err)
	}
	load["sourceUris"] = sources
	loadPayload, err := json.Marshal(load)
	if err != nil {
		return nil, nil, fmt.Errorf("encode media upload configuration: %w", err)
	}
	configuration["load"] = loadPayload
	root["configuration"], err = json.Marshal(configuration)
	if err != nil {
		return nil, nil, fmt.Errorf("encode media upload metadata: %w", err)
	}
	payload, err := json.Marshal(root)
	if err != nil {
		return nil, nil, fmt.Errorf("encode media upload job: %w", err)
	}
	return payload, loadPayload, nil
}

func (h *mediaUploadHandlers) objectName(projectID, id string) string {
	return ".bqemu-media/" + projectID + "/" + id + ".parquet"
}

func (h *mediaUploadHandlers) location(r *http.Request, id string) string {
	baseURL := h.baseURL
	if baseURL == "" {
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		baseURL = scheme + "://" + r.Host
	}
	values := url.Values{"uploadType": []string{"resumable"}, "upload_id": []string{id}}
	return baseURL + r.URL.Path + "?" + values.Encode()
}

func (h *mediaUploadHandlers) pruneLocked(now time.Time) {
	for id, session := range h.sessions {
		// Completion writes to the external object store without this mutex. Do
		// not let TTL cleanup discard the in-memory payload while that write is
		// still in flight.
		if session.finalizing || now.Sub(session.updatedAt) < h.support.config.SessionTTL {
			continue
		}
		h.used -= int64(len(session.payload))
		delete(h.sessions, id)
	}
}

// Completed sessions retain a small idempotency cache for a retried final
// request, but they are not active uploads. Keep that cache bounded without
// making a sequence of successful uploads consume the active session quota.
func (h *mediaUploadHandlers) trimCompletedLocked() {
	for h.completedSessionsLocked() >= h.support.config.MaxSessions {
		oldestID := ""
		var oldest time.Time
		for id, session := range h.sessions {
			if session.completed == nil || (oldestID != "" && !session.updatedAt.Before(oldest)) {
				continue
			}
			oldestID, oldest = id, session.updatedAt
		}
		if oldestID == "" {
			return
		}
		delete(h.sessions, oldestID)
	}
}

func (h *mediaUploadHandlers) activeSessionsLocked() int {
	active := 0
	for _, session := range h.sessions {
		if session.completed == nil {
			active++
		}
	}
	return active
}

func (h *mediaUploadHandlers) completedSessionsLocked() int {
	completed := 0
	for _, session := range h.sessions {
		if session.completed != nil {
			completed++
		}
	}
	return completed
}

// reserveMultipart reserves all capacity currently available to a multipart
// request before it reads either MIME part. Unlike a resumable chunk, a
// multipart request does not state the media part's byte count independently,
// so reserving the remaining shared budget prevents concurrent requests from
// allocating an unbounded number of MaxBytes-sized buffers.
func (h *mediaUploadHandlers) reserveMultipart() (int64, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.pruneLocked(h.support.now())
	available := h.support.config.MaxBytes - h.used - h.reserved
	if available < 1 {
		return 0, mediaProtocolError(http.StatusTooManyRequests, "rateLimitExceeded", "media upload memory budget is busy", domain.ErrPrecondition)
	}
	h.reserved += available
	return available, nil
}

func (h *mediaUploadHandlers) reserveBytes(size int64) error {
	if size < 1 {
		return mediaProtocolError(http.StatusBadRequest, "invalid", "resumable upload chunk is empty", domain.ErrInvalid)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.pruneLocked(h.support.now())
	if size > h.support.config.MaxBytes {
		return requestBodyTooLarge(fmt.Errorf("resumable upload exceeds configured byte limit"))
	}
	if size > h.support.config.MaxBytes-h.used-h.reserved {
		return mediaProtocolError(http.StatusTooManyRequests, "rateLimitExceeded", "media upload memory budget is busy", domain.ErrPrecondition)
	}
	h.reserved += size
	return nil
}

func (h *mediaUploadHandlers) releaseReservation(size int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.reserved -= size
	if h.reserved < 0 {
		panic("media upload reservation accounting underflow")
	}
}

func isJSONMediaType(value string) bool {
	mediaType, _, err := mime.ParseMediaType(value)
	return err == nil && strings.EqualFold(mediaType, "application/json")
}

func mediaContentType(value string) (string, error) {
	mediaType, _, err := mime.ParseMediaType(value)
	if err != nil || strings.TrimSpace(mediaType) == "" {
		return "", mediaProtocolError(http.StatusBadRequest, "invalid", "media content type is required", domain.ErrInvalid)
	}
	return mediaType, nil
}

func readMediaPart(reader io.Reader, maximum int64) ([]byte, error) {
	payload, err := io.ReadAll(io.LimitReader(reader, maximum+1))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return nil, requestBodyTooLarge(err)
		}
		return nil, fmt.Errorf("%w: read media upload body: %v", domain.ErrInvalid, err)
	}
	if int64(len(payload)) > maximum {
		return nil, requestBodyTooLarge(fmt.Errorf("media upload exceeds %d bytes", maximum))
	}
	return payload, nil
}

func readMediaMetadata(reader io.Reader) ([]byte, error) {
	payload, err := io.ReadAll(io.LimitReader(reader, mediaUploadMetadataLimit+1))
	if err != nil {
		return nil, fmt.Errorf("%w: read media upload metadata: %v", domain.ErrInvalid, err)
	}
	if len(payload) > mediaUploadMetadataLimit {
		return nil, requestBodyTooLarge(fmt.Errorf("media upload metadata exceeds %d bytes", mediaUploadMetadataLimit))
	}
	return payload, nil
}

func declaredUploadLength(value string, maximum int64) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return -1, nil
	}
	length, err := strconv.ParseInt(value, 10, 64)
	if err != nil || length < 0 {
		return 0, mediaProtocolError(http.StatusBadRequest, "invalid", "X-Upload-Content-Length must be a non-negative decimal integer", domain.ErrInvalid)
	}
	if length > maximum {
		return 0, requestBodyTooLarge(fmt.Errorf("media upload exceeds %d bytes", maximum))
	}
	return length, nil
}

type uploadContentRange struct {
	probe      bool
	start      int64
	end        int64
	total      int64
	totalKnown bool
}

func parseUploadContentRange(value string) (uploadContentRange, error) {
	const prefix = "bytes "
	value = strings.TrimSpace(value)
	if !strings.HasPrefix(strings.ToLower(value), prefix) {
		return uploadContentRange{}, mediaProtocolError(http.StatusBadRequest, "invalid", "Content-Range is required", domain.ErrInvalid)
	}
	value = strings.TrimSpace(value[len(prefix):])
	parts := strings.Split(value, "/")
	if len(parts) != 2 {
		return uploadContentRange{}, mediaProtocolError(http.StatusBadRequest, "invalid", "Content-Range is invalid", domain.ErrInvalid)
	}
	totalText := strings.TrimSpace(parts[1])
	total, totalKnown := int64(-1), totalText != "*"
	if totalKnown {
		var err error
		total, err = strconv.ParseInt(totalText, 10, 64)
		if err != nil || total < 0 {
			return uploadContentRange{}, mediaProtocolError(http.StatusBadRequest, "invalid", "Content-Range total is invalid", domain.ErrInvalid)
		}
	}
	if strings.TrimSpace(parts[0]) == "*" {
		return uploadContentRange{probe: true, total: total, totalKnown: totalKnown}, nil
	}
	rangeParts := strings.Split(parts[0], "-")
	if len(rangeParts) != 2 {
		return uploadContentRange{}, mediaProtocolError(http.StatusBadRequest, "invalid", "Content-Range byte range is invalid", domain.ErrInvalid)
	}
	start, startErr := strconv.ParseInt(strings.TrimSpace(rangeParts[0]), 10, 64)
	end, endErr := strconv.ParseInt(strings.TrimSpace(rangeParts[1]), 10, 64)
	if startErr != nil || endErr != nil || start < 0 || end < start || (totalKnown && end >= total) {
		return uploadContentRange{}, mediaProtocolError(http.StatusBadRequest, "invalid", "Content-Range byte range is invalid", domain.ErrInvalid)
	}
	rangeSpec := uploadContentRange{start: start, end: end, total: total, totalKnown: totalKnown}
	if _, err := rangeSpec.length(); err != nil {
		return uploadContentRange{}, err
	}
	return rangeSpec, nil
}

func (r uploadContentRange) length() (int64, error) {
	if r.probe || r.start < 0 || r.end < r.start || r.end == int64(^uint64(0)>>1) {
		return 0, mediaProtocolError(http.StatusBadRequest, "invalid", "Content-Range byte range is invalid", domain.ErrInvalid)
	}
	return r.end - r.start + 1, nil
}

func writeResumeIncomplete(w http.ResponseWriter, received int64) {
	if received > 0 {
		w.Header().Set("Range", fmt.Sprintf("bytes=0-%d", received-1))
	}
	w.WriteHeader(http.StatusPermanentRedirect)
}

func mediaProtocolError(status int, reason, message string, err error) error {
	return &httpProtocolError{status: status, reason: reason, message: message, err: err}
}

func writeMediaLoadError(w http.ResponseWriter, err error) {
	var protocol *httpProtocolError
	if errors.As(err, &protocol) {
		writeError(w, err)
		return
	}
	writeLoadError(w, err)
}
