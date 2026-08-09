package application

// ReadRows converts a stream-relative resume offset to an immutable snapshot
// range and checks every backend batch for gaps, overlap, and incorrect counts.
//
// Protocol sources:
//   - ReadRows offset: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#readrowsrequest
//   - Response row_count/progress: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#readrowsresponse

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/leeyh0216/go-bemu/internal/storageread/domain"
)

func (s *Service) ReadRows(ctx context.Context, request domain.ReadRowsRequest, send func(domain.ReadChunk) error) (result error) {
	const operation = "storage_read.read_rows"
	if request.StreamName == "" || request.Offset < 0 || send == nil {
		return domain.NewError(domain.ErrorInvalidArgument, operation, errors.New("stream, non-negative offset, and sender are required"))
	}

	s.mu.RLock()
	entry, found := s.streams[request.StreamName]
	if found {
		entry.session.mu.RLock()
	}
	s.mu.RUnlock()
	if !found {
		return s.persistedStreamError(ctx, request.StreamName)
	}
	defer entry.session.mu.RUnlock()
	if !s.clock.Now().Before(entry.session.session.ExpireTime) {
		return domain.NewError(domain.ErrorNotFound, operation, errors.New("read session expired"))
	}
	streamRows := entry.stream.RowCount()
	if request.Offset > streamRows {
		return domain.NewError(domain.ErrorInvalidArgument, operation, errors.New("offset exceeds stream row count"))
	}
	absoluteStart := entry.stream.StartOffset + request.Offset
	absoluteEnd := entry.stream.EndOffset
	s.logger.InfoContext(ctx, "opening read snapshot range",
		"event", "side_effect.before", "side_effect", "snapshot.open_range",
		"operation", operation, "model_version", s.config.ProtocolModelVersion,
		"stream", request.StreamName, "stream_offset", request.Offset,
		"absolute_start", absoluteStart, "absolute_end", absoluteEnd,
		"max_rows_per_response", s.config.MaxRowsPerResponse,
	)
	iterator, err := entry.session.snapshot.OpenRange(ctx, absoluteStart, absoluteEnd, s.config.MaxRowsPerResponse)
	if err != nil {
		s.logger.ErrorContext(ctx, "opening read snapshot range failed",
			"event", "side_effect.error", "side_effect", "snapshot.open_range",
			"operation", operation, "model_version", s.config.ProtocolModelVersion,
			"stream", request.StreamName,
			"error_type", fmt.Sprintf("%T", err), "error_digest", digest([]byte(err.Error())),
		)
		return domain.NewError(domain.ErrorInternal, operation, err)
	}
	if iterator == nil {
		err := errors.New("snapshot returned a nil batch iterator")
		s.logger.ErrorContext(ctx, "opening read snapshot range returned no iterator",
			"event", "side_effect.error", "side_effect", "snapshot.open_range",
			"operation", operation, "model_version", s.config.ProtocolModelVersion,
			"stream", request.StreamName,
			"error_type", fmt.Sprintf("%T", err), "error_digest", digest([]byte(err.Error())),
		)
		return domain.NewError(domain.ErrorInternal, operation, err)
	}
	s.logger.InfoContext(ctx, "read snapshot range opened",
		"event", "side_effect.after", "side_effect", "snapshot.open_range",
		"operation", operation, "model_version", s.config.ProtocolModelVersion,
		"stream", request.StreamName,
	)
	defer func() {
		s.logger.InfoContext(ctx, "closing read snapshot iterator",
			"event", "side_effect.before", "side_effect", "snapshot.iterator.close",
			"operation", operation, "model_version", s.config.ProtocolModelVersion,
			"stream", request.StreamName,
		)
		closeErr := iterator.Close()
		attrs := []any{
			"event", "side_effect.after", "side_effect", "snapshot.iterator.close",
			"operation", operation, "model_version", s.config.ProtocolModelVersion,
			"stream", request.StreamName, "success", closeErr == nil,
		}
		if closeErr != nil {
			attrs = append(attrs, "error_type", fmt.Sprintf("%T", closeErr), "error_digest", digest([]byte(closeErr.Error())))
			if result == nil {
				result = domain.NewError(domain.ErrorInternal, operation, closeErr)
			}
		}
		s.logger.InfoContext(ctx, "read snapshot iterator closed", attrs...)
	}()

	expected := absoluteStart
	first := true
	for {
		batch, nextErr := iterator.Next(ctx)
		if errors.Is(nextErr, io.EOF) {
			if expected != absoluteEnd {
				return domain.NewError(domain.ErrorInternal, operation, fmt.Errorf("snapshot ended at %d, expected %d", expected, absoluteEnd))
			}
			return nil
		}
		if nextErr != nil {
			s.logger.ErrorContext(ctx, "reading encoded snapshot batch failed",
				"event", "side_effect.error", "side_effect", "snapshot.iterator.next",
				"operation", operation, "model_version", s.config.ProtocolModelVersion,
				"stream", request.StreamName, "expected_offset", expected-entry.stream.StartOffset,
				"error_type", fmt.Sprintf("%T", nextErr), "error_digest", digest([]byte(nextErr.Error())),
			)
			return domain.NewError(domain.ErrorInternal, operation, nextErr)
		}
		if batch.Offset != expected || batch.RowCount <= 0 || batch.RowCount > s.config.MaxRowsPerResponse || batch.Offset+batch.RowCount > absoluteEnd {
			return domain.NewError(domain.ErrorInternal, operation, fmt.Errorf("non-contiguous snapshot batch at %d with %d rows; expected %d before %d", batch.Offset, batch.RowCount, expected, absoluteEnd))
		}
		chunk := domain.ReadChunk{
			Format:        entry.session.session.Format,
			Batch:         batch,
			ProgressStart: progress(entry.stream, batch.Offset),
			ProgressEnd:   progress(entry.stream, batch.Offset+batch.RowCount),
		}
		if first {
			schema := cloneSchema(entry.session.session.Schema)
			chunk.Schema = &schema
			first = false
		}
		s.logger.DebugContext(ctx, "sending encoded read batch",
			"event", "boundary.before", "boundary", "storage_read.chunk",
			"operation", operation, "model_version", s.config.ProtocolModelVersion,
			"stream", request.StreamName, "offset", batch.Offset-entry.stream.StartOffset,
			"row_count", batch.RowCount, "payload_bytes", len(batch.SerializedRows),
			"payload_digest", digest(batch.SerializedRows),
			"schema_fingerprint", entry.session.session.Schema.Fingerprint,
		)
		if err := send(chunk); err != nil {
			return err
		}
		expected += batch.RowCount
	}
}

func progress(stream domain.Stream, absoluteOffset int64) float64 {
	if stream.RowCount() == 0 {
		return 1
	}
	return float64(absoluteOffset-stream.StartOffset) / float64(stream.RowCount())
}
