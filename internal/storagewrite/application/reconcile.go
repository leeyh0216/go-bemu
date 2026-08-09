package application

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/leeyh0216/go-bemu/internal/storagewrite/domain"
	"github.com/leeyh0216/go-bemu/internal/storagewrite/ports"
)

// Reconcile compares canonical SQLite state with opaque DuckDB staging rows.
// It repairs only transitions whose direction is provable from the two stores;
// every ambiguous or unowned physical object fails startup with restoration
// guidance instead of silently discarding rows.
func (s *Service) Reconcile(ctx context.Context) error {
	records, err := s.repository.ListWriteStreams(ctx)
	if err != nil {
		return fmt.Errorf("list canonical Storage Write streams: %w", err)
	}
	receipts := make(map[string][]domain.AppendReceipt, len(records))
	expected := make([]ports.PhysicalExpectation, 0, len(records))
	for _, record := range records {
		items, err := s.repository.ListWriteAppendReceipts(ctx, record.Stream.Name)
		if err != nil {
			return fmt.Errorf("list canonical Storage Write receipts for stream fingerprint %s: %w", digest([]byte(record.Stream.Name)), err)
		}
		receipts[record.Stream.Name] = items
		var stagedBytes int64
		if record.Stream.Type == domain.StreamTypePending && record.Stream.State != domain.StreamStateCommitted {
			for _, receipt := range items {
				if receipt.State == domain.AppendReceiptApplied || record.Operation == domain.StreamOperationAppend &&
					record.OperationToken == strconv.FormatInt(receipt.StartOffset, 10) {
					stagedBytes += receipt.StagedBytes
				}
			}
		} else if record.Stream.Type == domain.StreamTypeDefault && record.Operation == domain.StreamOperationAppend {
			for _, receipt := range items {
				if record.OperationToken == strconv.FormatInt(receipt.StartOffset, 10) {
					stagedBytes = receipt.StagedBytes
					break
				}
			}
		}
		expected = append(expected, ports.PhysicalExpectation{StreamName: record.Stream.Name, StagedBytes: stagedBytes})
	}
	physical, err := s.coordinator.InspectPhysical(ctx, expected)
	if err != nil {
		return fmt.Errorf("inspect Storage Write physical state: %w", err)
	}

	commitGroups := make(map[string][]domain.StreamRecord)
	acknowledge := make([]string, 0)
	for _, record := range records {
		state := physical[record.Stream.Name]
		switch record.Operation {
		case domain.StreamOperationAppend:
			if err := s.reconcileAppend(ctx, record, receipts[record.Stream.Name], state); err != nil {
				return err
			}
			if state.AppliedExists {
				acknowledge = append(acknowledge, record.Stream.Name)
			}
		case domain.StreamOperationCommit:
			commitGroups[record.OperationToken] = append(commitGroups[record.OperationToken], record)
		case domain.StreamOperationNone:
			if err := validateSettledPhysicalState(record, state); err != nil {
				return err
			}
			if state.AppliedExists {
				acknowledge = append(acknowledge, record.Stream.Name)
			}
		default:
			return fmt.Errorf("Storage Write state drift: unknown operation %q for stream fingerprint %s", record.Operation, digest([]byte(record.Stream.Name)))
		}
	}

	groupTokens := make([]string, 0, len(commitGroups))
	for token := range commitGroups {
		groupTokens = append(groupTokens, token)
	}
	sort.Strings(groupTokens)
	for _, token := range groupTokens {
		groupAck, err := s.reconcileCommit(ctx, token, commitGroups[token], physical)
		if err != nil {
			return err
		}
		acknowledge = append(acknowledge, groupAck...)
	}
	if len(acknowledge) != 0 {
		sort.Strings(acknowledge)
		acknowledge = compactStrings(acknowledge)
		if err := s.coordinator.AcknowledgeApplied(ctx, acknowledge); err != nil {
			return fmt.Errorf("clean reconciled Storage Write applied rows: %w", err)
		}
	}
	return nil
}

func (s *Service) reconcileAppend(ctx context.Context, record domain.StreamRecord, receipts []domain.AppendReceipt, physical ports.PhysicalStreamState) error {
	offset, err := strconv.ParseInt(record.OperationToken, 10, 64)
	if err != nil {
		return fmt.Errorf("Storage Write state drift: invalid append token for stream fingerprint %s", digest([]byte(record.Stream.Name)))
	}
	var receipt domain.AppendReceipt
	found := false
	for _, candidate := range receipts {
		if candidate.StartOffset == offset {
			receipt, found = candidate, true
			break
		}
	}
	if !found || receipt.State != domain.AppendReceiptPrepared {
		return fmt.Errorf("Storage Write state drift: prepared receipt is missing for stream fingerprint %s", digest([]byte(record.Stream.Name)))
	}
	if record.Stream.Type == domain.StreamTypeDefault {
		if physical.PendingExists && physical.AppliedExists {
			return storageWritePhysicalDrift(record, "both pending and applied default rows exist")
		}
		switch {
		case physical.AppliedExists:
			if physical.AppliedRows != receipt.RowCount {
				return storageWritePhysicalDrift(record, fmt.Sprintf("applied default rows = %d, expected %d", physical.AppliedRows, receipt.RowCount))
			}
			return s.completeReconciledAppend(ctx, record, receipt)
		case physical.PendingExists:
			if physical.PendingRows != receipt.RowCount {
				return storageWritePhysicalDrift(record, fmt.Sprintf("pending default rows = %d, expected %d", physical.PendingRows, receipt.RowCount))
			}
			if err := s.coordinator.CommitPending(ctx, ports.CommitRequest{
				Parent: record.Stream.Parent, StreamNames: []string{record.Stream.Name},
				RowCounts: map[string]int64{record.Stream.Name: receipt.RowCount},
			}); err != nil {
				return fmt.Errorf("resume default Storage Write visibility: %w", err)
			}
			return s.completeReconciledAppend(ctx, record, receipt)
		default:
			return s.abortReconciledAppend(ctx, record, receipt)
		}
	}
	if physical.AppliedExists {
		return storageWritePhysicalDrift(record, "pending stream has an applied marker outside commit")
	}
	beforeRows := record.Stream.RowCount
	switch {
	case !physical.PendingExists && beforeRows == 0:
		return s.abortReconciledAppend(ctx, record, receipt)
	case physical.PendingExists && physical.PendingRows == beforeRows:
		return s.abortReconciledAppend(ctx, record, receipt)
	case physical.PendingExists && physical.PendingRows == beforeRows+receipt.RowCount:
		return s.completeReconciledAppend(ctx, record, receipt)
	default:
		return storageWritePhysicalDrift(record, fmt.Sprintf("pending rows = %d, expected %d or %d", physical.PendingRows, beforeRows, beforeRows+receipt.RowCount))
	}
}

func (s *Service) completeReconciledAppend(ctx context.Context, record domain.StreamRecord, receipt domain.AppendReceipt) error {
	expected := record.Revision
	record.Operation = domain.StreamOperationNone
	record.OperationToken = ""
	record.Stream.RowCount += receipt.RowCount
	record.Stream.NextOffset += receipt.RowCount
	record.Stream.LastActivity = s.clock.Now()
	record.Revision++
	receipt.State = domain.AppendReceiptApplied
	receipt.UpdatedAt = record.Stream.LastActivity
	if err := s.repository.CompleteAppend(ctx, expected, record, receipt); err != nil {
		return fmt.Errorf("complete reconciled Storage Write append: %w", err)
	}
	return nil
}

func (s *Service) abortReconciledAppend(ctx context.Context, record domain.StreamRecord, receipt domain.AppendReceipt) error {
	expected := record.Revision
	record.Operation = domain.StreamOperationNone
	record.OperationToken = ""
	record.Stream.LastActivity = s.clock.Now()
	record.Revision++
	if err := s.repository.AbortAppend(ctx, expected, record, receipt); err != nil {
		return fmt.Errorf("abort unapplied Storage Write append: %w", err)
	}
	return nil
}

func (s *Service) reconcileCommit(ctx context.Context, token string, records []domain.StreamRecord, physical map[string]ports.PhysicalStreamState) ([]string, error) {
	sort.Slice(records, func(i, j int) bool { return records[i].Stream.Name < records[j].Stream.Name })
	names := make([]string, len(records))
	allPending, allApplied := true, true
	for index, record := range records {
		names[index] = record.Stream.Name
		if record.Stream.State != domain.StreamStateFinalized || record.OperationToken != token {
			return nil, storageWritePhysicalDrift(record, "commit operation has invalid canonical state")
		}
		state := physical[record.Stream.Name]
		pendingMatches := state.PendingRows == record.Stream.RowCount && (state.PendingExists || record.Stream.RowCount == 0)
		appliedMatches := state.AppliedExists && state.AppliedRows == record.Stream.RowCount
		if state.PendingExists && state.AppliedExists {
			return nil, storageWritePhysicalDrift(record, "both pending and applied commit rows exist")
		}
		allPending = allPending && pendingMatches && !state.AppliedExists
		allApplied = allApplied && appliedMatches && !state.PendingExists
	}
	if allPending {
		expected := make(map[string]int64, len(records))
		aborted := make([]domain.StreamRecord, len(records))
		for index, record := range records {
			expected[record.Stream.Name] = record.Revision
			aborted[index] = record
			aborted[index].Operation = domain.StreamOperationNone
			aborted[index].OperationToken = ""
			aborted[index].Revision++
		}
		if err := s.repository.SaveWriteStreams(ctx, expected, aborted); err != nil {
			return nil, fmt.Errorf("abort unapplied Storage Write commit: %w", err)
		}
		return nil, nil
	}
	if !allApplied {
		return nil, fmt.Errorf("Storage Write state drift: commit group %s is only partially applied; restore matching SQLite and DuckDB files", token)
	}
	commitTime := s.clock.Now()
	expected := make(map[string]int64, len(records))
	completed := make([]domain.StreamRecord, len(records))
	for index, record := range records {
		expected[record.Stream.Name] = record.Revision
		completed[index] = record
		completed[index].Stream.State = domain.StreamStateCommitted
		completed[index].Stream.CommitTime = cloneTime(&commitTime)
		completed[index].Stream.LastActivity = commitTime
		completed[index].Operation = domain.StreamOperationNone
		completed[index].OperationToken = ""
		completed[index].Revision++
	}
	if err := s.repository.SaveWriteStreams(ctx, expected, completed); err != nil {
		return nil, fmt.Errorf("complete reconciled Storage Write commit: %w", err)
	}
	return names, nil
}

func validateSettledPhysicalState(record domain.StreamRecord, physical ports.PhysicalStreamState) error {
	if physical.PendingExists && physical.AppliedExists {
		return storageWritePhysicalDrift(record, "both pending and applied rows exist")
	}
	if record.Stream.Type == domain.StreamTypeDefault || record.Stream.State == domain.StreamStateCommitted {
		if physical.PendingExists {
			return storageWritePhysicalDrift(record, "settled stream retains pending rows")
		}
		return nil
	}
	if physical.AppliedExists {
		return storageWritePhysicalDrift(record, "uncommitted stream retains applied rows")
	}
	if record.Stream.RowCount == 0 {
		if physical.PendingExists && physical.PendingRows != 0 {
			return storageWritePhysicalDrift(record, "empty stream has physical rows")
		}
		return nil
	}
	if !physical.PendingExists || physical.PendingRows != record.Stream.RowCount {
		return storageWritePhysicalDrift(record, fmt.Sprintf("staged rows = %d, expected %d", physical.PendingRows, record.Stream.RowCount))
	}
	return nil
}

func storageWritePhysicalDrift(record domain.StreamRecord, detail string) error {
	return fmt.Errorf("Storage Write state drift for stream fingerprint %s: %s; restore matching SQLite and DuckDB files",
		digest([]byte(record.Stream.Name)), detail)
}

func compactStrings(values []string) []string {
	if len(values) < 2 {
		return values
	}
	result := values[:1]
	for _, value := range values[1:] {
		if value != result[len(result)-1] {
			result = append(result, value)
		}
	}
	return result
}
