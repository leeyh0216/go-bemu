package memory

import (
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/leeyh0216/go-bemu/internal/storagewrite/domain"
	"github.com/leeyh0216/go-bemu/internal/storagewrite/ports"
)

var _ ports.StreamRepository = (*Repository)(nil)

type Repository struct {
	mu       sync.Mutex
	streams  map[string]domain.StreamRecord
	receipts map[string]map[int64]domain.AppendReceipt
}

func NewRepository() *Repository {
	return &Repository{streams: make(map[string]domain.StreamRecord), receipts: make(map[string]map[int64]domain.AppendReceipt)}
}

func (r *Repository) CreateWriteStream(ctx context.Context, record domain.StreamRecord, maxPending int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.streams[record.Stream.Name]; exists {
		return ports.ErrStreamExists
	}
	if record.Stream.Type == domain.StreamTypePending && maxPending > 0 {
		var count int64
		for _, current := range r.streams {
			if current.Stream.Type == domain.StreamTypePending && current.Stream.State != domain.StreamStateCommitted && current.Stream.State != domain.StreamStateFailed {
				count++
			}
		}
		if count >= maxPending {
			return ports.ErrResourceExhausted
		}
	}
	r.streams[record.Stream.Name] = cloneRecord(record)
	return nil
}

func (r *Repository) GetWriteStream(ctx context.Context, name string) (domain.StreamRecord, error) {
	if err := ctx.Err(); err != nil {
		return domain.StreamRecord{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	record, exists := r.streams[name]
	if !exists {
		return domain.StreamRecord{}, ports.ErrStreamNotFound
	}
	return cloneRecord(record), nil
}

func (r *Repository) ListWriteStreams(ctx context.Context) ([]domain.StreamRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]domain.StreamRecord, 0, len(r.streams))
	for _, record := range r.streams {
		result = append(result, cloneRecord(record))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Stream.Name < result[j].Stream.Name })
	return result, nil
}

func (r *Repository) CountActivePendingStreams(ctx context.Context) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var count int64
	for _, record := range r.streams {
		if record.Stream.Type == domain.StreamTypePending && record.Stream.State != domain.StreamStateCommitted && record.Stream.State != domain.StreamStateFailed {
			count++
		}
	}
	return count, nil
}

func (r *Repository) SaveWriteStream(ctx context.Context, expected int64, record domain.StreamRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.save(expected, record)
}

func (r *Repository) SaveWriteStreams(ctx context.Context, expected map[string]int64, records []domain.StreamRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, record := range records {
		current, exists := r.streams[record.Stream.Name]
		if !exists || current.Revision != expected[record.Stream.Name] {
			return ports.ErrStreamConflict
		}
	}
	for _, record := range records {
		r.streams[record.Stream.Name] = cloneRecord(record)
	}
	return nil
}

func (r *Repository) DeleteWriteStream(ctx context.Context, name string, expected int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current, exists := r.streams[name]
	if !exists || current.Revision != expected {
		return ports.ErrStreamConflict
	}
	delete(r.streams, name)
	delete(r.receipts, name)
	return nil
}

func (r *Repository) PrepareAppend(ctx context.Context, expected int64, record domain.StreamRecord, receipt domain.AppendReceipt) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(expected, record.Stream.Name); err != nil {
		return err
	}
	byOffset := r.receipts[receipt.StreamName]
	if byOffset == nil {
		byOffset = make(map[int64]domain.AppendReceipt)
		r.receipts[receipt.StreamName] = byOffset
	}
	if _, exists := byOffset[receipt.StartOffset]; exists {
		return ports.ErrReceiptConflict
	}
	byOffset[receipt.StartOffset] = receipt
	r.streams[record.Stream.Name] = cloneRecord(record)
	return nil
}

func (r *Repository) CompleteAppend(ctx context.Context, expected int64, record domain.StreamRecord, receipt domain.AppendReceipt) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(expected, record.Stream.Name); err != nil {
		return err
	}
	current, exists := r.receipts[receipt.StreamName][receipt.StartOffset]
	if !exists || current.State != domain.AppendReceiptPrepared || !sameReceiptIdentity(current, receipt) {
		return ports.ErrReceiptConflict
	}
	r.receipts[receipt.StreamName][receipt.StartOffset] = receipt
	r.streams[record.Stream.Name] = cloneRecord(record)
	return nil
}

func (r *Repository) AbortAppend(ctx context.Context, expected int64, record domain.StreamRecord, receipt domain.AppendReceipt) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.check(expected, record.Stream.Name); err != nil {
		return err
	}
	current, exists := r.receipts[receipt.StreamName][receipt.StartOffset]
	if !exists || current.State != domain.AppendReceiptPrepared || !sameReceiptIdentity(current, receipt) {
		return ports.ErrReceiptConflict
	}
	delete(r.receipts[receipt.StreamName], receipt.StartOffset)
	r.streams[record.Stream.Name] = cloneRecord(record)
	return nil
}

func (r *Repository) GetWriteAppendReceipt(ctx context.Context, stream string, offset int64) (domain.AppendReceipt, error) {
	if err := ctx.Err(); err != nil {
		return domain.AppendReceipt{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	receipt, exists := r.receipts[stream][offset]
	if !exists {
		return domain.AppendReceipt{}, ports.ErrReceiptNotFound
	}
	return receipt, nil
}

func (r *Repository) ListWriteAppendReceipts(ctx context.Context, stream string) ([]domain.AppendReceipt, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]domain.AppendReceipt, 0, len(r.receipts[stream]))
	for _, receipt := range r.receipts[stream] {
		result = append(result, receipt)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].StartOffset < result[j].StartOffset })
	return result, nil
}

func (r *Repository) check(expected int64, name string) error {
	current, exists := r.streams[name]
	if !exists || current.Revision != expected {
		return ports.ErrStreamConflict
	}
	return nil
}

func (r *Repository) save(expected int64, record domain.StreamRecord) error {
	if err := r.check(expected, record.Stream.Name); err != nil {
		return err
	}
	r.streams[record.Stream.Name] = cloneRecord(record)
	return nil
}

func sameReceiptIdentity(left, right domain.AppendReceipt) bool {
	return left.StreamName == right.StreamName && left.StartOffset == right.StartOffset &&
		left.RowCount == right.RowCount && left.StagedBytes == right.StagedBytes &&
		left.SchemaFingerprint == right.SchemaFingerprint && left.PayloadDigest == right.PayloadDigest
}

func cloneRecord(record domain.StreamRecord) domain.StreamRecord {
	record.WriterDescriptor = append([]byte(nil), record.WriterDescriptor...)
	record.Stream.Schema.Fields = cloneFields(record.Stream.Schema.Fields)
	if record.Stream.CommitTime != nil {
		value := *record.Stream.CommitTime
		record.Stream.CommitTime = &value
	}
	return record
}

func cloneFields(fields []domain.Field) []domain.Field {
	result := make([]domain.Field, len(fields))
	for index, field := range fields {
		result[index] = field
		if field.Precision != nil {
			value := *field.Precision
			result[index].Precision = &value
		}
		if field.Scale != nil {
			value := *field.Scale
			result[index].Scale = &value
		}
		result[index].Fields = cloneFields(field.Fields)
	}
	return result
}

var _ = errors.Is
