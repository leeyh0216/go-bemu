package application

// Contract helpers keep wire-derived resource and partition rules separate
// from orchestration. The backend supplies bytes, but these invariants prevent
// duplicate or missing rows across logical streams.
//
// Protocol sources:
//   - ReadStream resource/range semantics: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#readstream
//   - Arrow/Avro schema messages: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#readsession

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/leeyh0216/go-bemu/internal/storageread/domain"
)

func (s *Service) negotiateStreamCount(maximum, preferred int32) (int32, error) {
	if maximum < 0 || preferred < 0 {
		return 0, errors.New("stream counts must not be negative")
	}
	if maximum > 0 && preferred > maximum {
		return 0, errors.New("preferred minimum stream count exceeds maximum")
	}
	upper := s.config.MaxStreams
	if maximum > 0 && maximum < upper {
		upper = maximum
	}
	chosen := upper
	if maximum == 0 {
		chosen = s.config.DefaultStreamCount
	}
	if preferred > 0 {
		chosen = max(preferred, s.config.DefaultStreamCount)
	}
	if chosen > upper {
		chosen = upper
	}
	return chosen, nil
}

func validateCreateRequest(request domain.CreateSessionRequest) error {
	if !validParent(request.Parent) {
		return errors.New("parent must have the form projects/{project}")
	}
	if !validTable(request.Table) {
		return errors.New("table must have the form projects/{project}/datasets/{dataset}/tables/{table}")
	}
	if request.Format != domain.FormatArrow && request.Format != domain.FormatAvro {
		return errors.New("data format must be ARROW or AVRO")
	}
	if len(request.TraceID) > 256 {
		return errors.New("trace ID exceeds 256 bytes")
	}
	for _, field := range request.SelectedFields {
		if strings.TrimSpace(field) == "" {
			return errors.New("selected fields must not contain empty names")
		}
	}
	return nil
}

func validateSnapshotMetadata(format domain.Format, metadata *domain.SnapshotMetadata) error {
	if metadata.RowCount < 0 || metadata.EstimatedBytes < 0 {
		return errors.New("snapshot metadata contains a negative size")
	}
	if metadata.Schema.Format != format {
		return fmt.Errorf("snapshot format %s does not match request %s", metadata.Schema.Format, format)
	}
	if len(metadata.Schema.Serialized) == 0 {
		return errors.New("snapshot reference schema is empty")
	}
	if format == domain.FormatAvro && !json.Valid(metadata.Schema.Serialized) {
		return errors.New("snapshot Avro reference schema is not JSON")
	}
	metadata.Schema.Serialized = slices.Clone(metadata.Schema.Serialized)
	metadata.Schema.Fingerprint = digest(metadata.Schema.Serialized)
	return nil
}

func partitionStreams(sessionName string, rows int64, count int32) []domain.Stream {
	streams := make([]domain.Stream, 0, count)
	base := rows / int64(count)
	remainder := rows % int64(count)
	start := int64(0)
	for index := int32(0); index < count; index++ {
		length := base
		if int64(index) < remainder {
			length++
		}
		end := start + length
		streams = append(streams, domain.Stream{
			Name:        fmt.Sprintf("%s/streams/%d", sessionName, index),
			StartOffset: start,
			EndOffset:   end,
		})
		start = end
	}
	return streams
}

func validParent(value string) bool {
	parts := strings.Split(value, "/")
	return len(parts) == 2 && parts[0] == "projects" && validResourceSegment(parts[1])
}

func validTable(value string) bool {
	parts := strings.Split(value, "/")
	return len(parts) == 6 && parts[0] == "projects" && validResourceSegment(parts[1]) &&
		parts[2] == "datasets" && validResourceSegment(parts[3]) &&
		parts[4] == "tables" && validResourceSegment(parts[5])
}

func validResourceSegment(value string) bool {
	if value == "" || len(value) > 256 {
		return false
	}
	for _, character := range value {
		if character == '/' || character == '\\' || character <= 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}

func cloneSchema(schema domain.ReferenceSchema) domain.ReferenceSchema {
	schema.Serialized = slices.Clone(schema.Serialized)
	return schema
}

func cloneSession(session domain.Session) domain.Session {
	session.Schema = cloneSchema(session.Schema)
	session.Streams = slices.Clone(session.Streams)
	session.SelectedFields = slices.Clone(session.SelectedFields)
	session.SnapshotTime = cloneTime(session.SnapshotTime)
	return session
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func digest(payload []byte) string {
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}
