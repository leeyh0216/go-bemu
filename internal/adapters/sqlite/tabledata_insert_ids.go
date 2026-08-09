package sqlite

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

const maxTableDataInsertIDsPerTable = 10_000

var _ ports.TableDataInsertIDLedger = (*Store)(nil)

func (s *Store) ExistingTableDataInsertIDs(ctx context.Context, reference domain.TableReference, ids []string) (map[string]bool, error) {
	result := make(map[string]bool)
	ids = uniqueTableDataInsertIDs(ids)
	if len(ids) == 0 {
		return result, nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	args := make([]any, 0, len(ids)+3)
	args = append(args, reference.ProjectID, reference.DatasetID, reference.TableID)
	for _, id := range ids {
		args = append(args, id)
	}
	rows, err := s.db.QueryContext(ctx, "SELECT insert_id FROM tabledata_insert_ids WHERE project_id = ? AND dataset_id = ? AND table_id = ? AND insert_id IN ("+placeholders+")", args...)
	if err != nil {
		return nil, fmt.Errorf("read tabledata insert IDs: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan tabledata insert ID: %w", err)
		}
		result[id] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tabledata insert IDs: %w", err)
	}
	return result, nil
}

func (s *Store) RecordTableDataInsertIDs(ctx context.Context, reference domain.TableReference, ids []string) error {
	ids = uniqueTableDataInsertIDs(ids)
	if len(ids) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tabledata insert ID transaction: %w", err)
	}
	defer tx.Rollback()
	now := time.Now().UnixNano()
	for _, id := range ids {
		if len(id) > 1024 {
			return fmt.Errorf("%w: insertId exceeds 1024 bytes", domain.ErrInvalid)
		}
		if _, err := tx.ExecContext(ctx, "INSERT OR IGNORE INTO tabledata_insert_ids (project_id, dataset_id, table_id, insert_id, created_at_ns) VALUES (?, ?, ?, ?, ?)", reference.ProjectID, reference.DatasetID, reference.TableID, id, now); err != nil {
			return fmt.Errorf("record tabledata insert ID: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM tabledata_insert_ids WHERE rowid IN (
		SELECT rowid FROM tabledata_insert_ids WHERE project_id = ? AND dataset_id = ? AND table_id = ?
		ORDER BY created_at_ns DESC, rowid DESC LIMIT -1 OFFSET ?
	)`, reference.ProjectID, reference.DatasetID, reference.TableID, maxTableDataInsertIDsPerTable); err != nil {
		return fmt.Errorf("bound tabledata insert ID ledger: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tabledata insert ID transaction: %w", err)
	}
	return nil
}

func uniqueTableDataInsertIDs(ids []string) []string {
	seen := make(map[string]struct{}, len(ids))
	result := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			result = append(result, id)
		}
	}
	return result
}
