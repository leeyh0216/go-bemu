package duckdb

// BigQuery tabledata.list protocol rules:
//   - https://cloud.google.com/bigquery/docs/reference/rest/v2/tabledata/list
//
// DuckDB paging stays behind TableDataReader. Count and page selection execute
// in one transaction so TotalRows and returned ordinals share one snapshot.

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/observability"
	"github.com/leeyh0216/go-bemu/internal/ports"
	tabledatabudget "github.com/leeyh0216/go-bemu/internal/tabledata"
)

const tableDataModelVersion = "duckdb-tabledata-canonical-page-v3"

var _ ports.TableDataReader = (*Warehouse)(nil)

func (w *Warehouse) ListTableData(ctx context.Context, request ports.TableDataReadRequest) (page ports.TableDataPage, err error) {
	reference, offset, limit := request.Reference, request.Offset, request.Limit
	budget := tabledatabudget.NewAccumulator(request.MaxResponseBytes)
	referenceSummary := reference.ProjectID + "\x00" + reference.DatasetID + "\x00" + reference.TableID
	started := observability.LogSideEffectStart(ctx, "duckdb", "list_table_data",
		"table_reference_digest", observability.Digest([]byte(referenceSummary)),
		"offset", offset, "limit", limit, "transaction_mode", "explicit",
		"model_version", tableDataModelVersion)
	defer func() {
		metrics := budget.Metrics()
		observability.LogSideEffectEnd(ctx, "duckdb", "list_table_data", started, err,
			"table_reference_digest", observability.Digest([]byte(referenceSummary)),
			"offset", offset, "limit", limit, "row_count", len(page.Rows), "total_rows", page.TotalRows,
			"result_bytes", metrics.Bytes, "result_digest", metrics.Digest,
			"transaction_mode", "explicit", "model_version", tableDataModelVersion)
	}()

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return ports.TableDataPage{}, fmt.Errorf("begin table data transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	tableName := quoteIdentifier(physicalSchema(reference.ProjectID, reference.DatasetID)) + "." + quoteIdentifier(reference.TableID)
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+tableName).Scan(&page.TotalRows); err != nil {
		return ports.TableDataPage{}, fmt.Errorf("count table data rows: %w", err)
	}
	// Backend JSON is not a protocol byte representation: to_json(row) includes
	// column names, while tabledata.list rows contain only schema-ordered f/v
	// cells. It must therefore never reject or trim a valid public response.
	// The configured row count bounds this query; canonical values and the REST
	// f/v encoder below the port remain the only authoritative byte gates.
	// https://cloud.google.com/bigquery/docs/reference/rest/v2/TableRow
	// https://cloud.google.com/bigquery/docs/paging-results#api-limits
	rows, err := tx.QueryContext(ctx, "SELECT * FROM "+tableName+" ORDER BY rowid LIMIT ? OFFSET ?", limit, offset)
	if err != nil {
		return ports.TableDataPage{}, fmt.Errorf("read table data page: %w", err)
	}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		_ = rows.Close()
		return ports.TableDataPage{}, fmt.Errorf("read table data schema: %w", err)
	}
	for rows.Next() {
		values := make([]any, len(columnTypes))
		destinations := make([]any, len(columnTypes))
		for index := range values {
			destinations[index] = &values[index]
		}
		if err = rows.Scan(destinations...); err != nil {
			_ = rows.Close()
			return ports.TableDataPage{}, fmt.Errorf("scan table data row: %w", err)
		}
		normalized, normalizeErr := normalizeSnapshotRow(request.Schema, values)
		if normalizeErr != nil {
			_ = rows.Close()
			return ports.TableDataPage{}, fmt.Errorf("normalize table data row: %w", normalizeErr)
		}
		canonical := tableDataCanonicalRow(request.Schema, normalized)
		included, budgetErr := budget.Add(canonical, request.MaxRowBytes)
		if budgetErr != nil {
			_ = rows.Close()
			return ports.TableDataPage{}, budgetErr
		}
		if !included {
			break
		}
		page.Rows = append(page.Rows, canonical)
	}
	if err = rows.Err(); err != nil {
		_ = rows.Close()
		return ports.TableDataPage{}, fmt.Errorf("iterate table data rows: %w", err)
	}
	if err = rows.Close(); err != nil {
		return ports.TableDataPage{}, fmt.Errorf("close table data rows: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return ports.TableDataPage{}, fmt.Errorf("commit table data transaction: %w", err)
	}
	return page, nil
}

// tableDataCanonicalRow removes all DuckDB driver-owned values at the outbound
// adapter boundary. The REST adapter receives only stable Go scalars, slices,
// maps, and BigQuery wire literals, so replacing DuckDB cannot affect JSON
// encoding behavior.
func tableDataCanonicalRow(fields []domain.Field, values []snapshotValue) []any {
	row := make([]any, len(fields))
	for index := range fields {
		row[index] = tableDataCanonicalValue(fields[index], values[index])
	}
	return row
}

func tableDataCanonicalValue(field domain.Field, value snapshotValue) any {
	if value.Null {
		return nil
	}
	if strings.EqualFold(field.Mode, "REPEATED") {
		element := field
		element.Mode = "REQUIRED"
		items := make([]any, len(value.Children))
		for index := range value.Children {
			items[index] = tableDataCanonicalValue(element, value.Children[index])
		}
		return items
	}
	switch strings.ToUpper(field.Type) {
	case "BOOL", "BOOLEAN":
		return value.Bool
	case "INT64", "INTEGER":
		return value.Int
	case "FLOAT64", "FLOAT":
		return value.Float
	case "NUMERIC", "BIGNUMERIC", "STRING", "JSON", "DATETIME":
		return value.Text
	case "BYTES":
		return bytes.Clone(value.Bytes)
	case "DATE":
		return time.Unix(value.Int*86400, 0).UTC().Format("2006-01-02")
	case "TIME":
		return formatTableDataTime(value.Int)
	case "TIMESTAMP":
		// Preserve epoch microseconds at the outbound port. The REST adapter owns
		// the formatOptions.useInt64Timestamp choice made by each caller.
		return value.Int
	case "RECORD", "STRUCT":
		children := make(map[string]any, len(field.Fields))
		for index, child := range field.Fields {
			children[child.Name] = tableDataCanonicalValue(child, value.Children[index])
		}
		return children
	default:
		return value.Text
	}
}

func formatTableDataTime(micros int64) string {
	hours := micros / int64(time.Hour/time.Microsecond)
	micros %= int64(time.Hour / time.Microsecond)
	minutes := micros / int64(time.Minute/time.Microsecond)
	micros %= int64(time.Minute / time.Microsecond)
	seconds := micros / int64(time.Second/time.Microsecond)
	fraction := micros % int64(time.Second/time.Microsecond)
	base := fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds)
	if fraction == 0 {
		return base
	}
	return base + "." + strings.TrimRight(fmt.Sprintf("%06d", fraction), "0")
}
