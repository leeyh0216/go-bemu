package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/leeyh0216/go-bemu/internal/adapters/sqlite/sqlcgen"
	"github.com/leeyh0216/go-bemu/internal/loadjob/domain"
	"github.com/leeyh0216/go-bemu/internal/loadjob/ports"
)

type loadMutationJournal struct {
	queries *sqlcgen.Queries
}

func newLoadMutationJournal(db *sql.DB) *loadMutationJournal {
	return &loadMutationJournal{queries: sqlcgen.New(db)}
}

var _ ports.MutationJournal = (*loadMutationJournal)(nil)

func (journal *loadMutationJournal) Prepare(ctx context.Context, record domain.MutationRecord) error {
	if err := record.Validate(); err != nil || record.Phase != domain.MutationPrepared {
		return fmt.Errorf("%w: prepared load mutation is invalid", domain.ErrInvalid)
	}
	destinationJSON, beforeSchemaJSON, err := encodeLoadMutationJSON(record)
	if err != nil {
		return err
	}
	err = journal.queries.CreateLoadMutation(ctx, sqlcgen.CreateLoadMutationParams{
		MutationID: record.ID, ProjectID: record.Job.ProjectID,
		LocationKey: strings.ToUpper(record.Job.Location), Location: record.Job.Location,
		JobID: record.Job.JobID, ConfigurationDigest: record.ConfigurationDigest,
		PlanFingerprint: record.PlanFingerprint, DestinationJson: destinationJSON,
		BeforeSchemaJson: beforeSchemaJSON, Publication: string(record.Publication),
		InputFiles: record.InputFiles, InputBytes: record.InputBytes,
	})
	if err == nil {
		return nil
	}
	if !sqliteConstraint(err) {
		return fmt.Errorf("prepare SQLite load mutation: %w", err)
	}
	existing, getErr := journal.get(ctx, record.ID)
	if getErr == nil && equivalentLoadMutationRecords(existing, record) {
		return nil
	}
	return fmt.Errorf("%w: load mutation identity already exists", domain.ErrConflict)
}

func (journal *loadMutationJournal) MarkPhysical(
	ctx context.Context,
	id, planFingerprint string,
	result ports.LoadResult,
) error {
	existing, err := journal.get(ctx, id)
	if err != nil {
		return err
	}
	validated := existing.Clone()
	validated.Phase = domain.MutationPhysical
	validated.Result = &domain.MutationResult{
		OutputRows: result.OutputRows, CreatedDestination: result.CreatedDestination,
		UpdatedDestination: result.UpdatedDestination,
	}
	if validated.PlanFingerprint != planFingerprint {
		return fmt.Errorf("%w: physical load mutation receipt differs", domain.ErrConflict)
	}
	if err := validated.Validate(); err != nil {
		return err
	}
	updated, err := journal.queries.MarkLoadMutationPhysical(ctx, sqlcgen.MarkLoadMutationPhysicalParams{
		OutputRows:         sql.NullInt64{Int64: result.OutputRows, Valid: true},
		CreatedDestination: sql.NullInt64{Int64: encodeBool(result.CreatedDestination), Valid: true},
		UpdatedDestination: sql.NullInt64{Int64: encodeBool(result.UpdatedDestination), Valid: true},
		MutationID:         id,
		PlanFingerprint:    planFingerprint,
	})
	if err != nil {
		return fmt.Errorf("mark SQLite load mutation physical: %w", err)
	}
	if updated == 1 {
		return nil
	}
	record, getErr := journal.get(ctx, id)
	want := domain.MutationResult{
		OutputRows: result.OutputRows, CreatedDestination: result.CreatedDestination,
		UpdatedDestination: result.UpdatedDestination,
	}
	if getErr == nil && (record.Phase == domain.MutationPhysical || record.Phase == domain.MutationApplied) &&
		record.PlanFingerprint == planFingerprint && record.Result != nil && *record.Result == want {
		return nil
	}
	if errors.Is(getErr, domain.ErrNotFound) {
		return getErr
	}
	return fmt.Errorf("%w: physical load mutation receipt differs", domain.ErrConflict)
}

func (journal *loadMutationJournal) MarkApplied(ctx context.Context, id string) error {
	updated, err := journal.queries.MarkLoadMutationApplied(ctx, id)
	if err != nil {
		return fmt.Errorf("mark SQLite load mutation applied: %w", err)
	}
	if updated == 1 {
		return nil
	}
	record, getErr := journal.get(ctx, id)
	if getErr == nil && record.Phase == domain.MutationApplied {
		return nil
	}
	if errors.Is(getErr, domain.ErrNotFound) {
		return getErr
	}
	return fmt.Errorf("%w: load mutation cannot become applied", domain.ErrConflict)
}

func (journal *loadMutationJournal) MarkAborted(ctx context.Context, id string) error {
	updated, err := journal.queries.MarkLoadMutationAborted(ctx, id)
	if err != nil {
		return fmt.Errorf("mark SQLite load mutation aborted: %w", err)
	}
	if updated == 1 {
		return nil
	}
	record, getErr := journal.get(ctx, id)
	if getErr == nil && record.Phase == domain.MutationAborted {
		return nil
	}
	if errors.Is(getErr, domain.ErrNotFound) {
		return getErr
	}
	return fmt.Errorf("%w: physical load mutation cannot be aborted", domain.ErrConflict)
}

func (journal *loadMutationJournal) ListRecoverable(ctx context.Context) ([]domain.MutationRecord, error) {
	rows, err := journal.queries.ListRecoverableLoadMutations(ctx)
	if err != nil {
		return nil, fmt.Errorf("list recoverable SQLite load mutations: %w", err)
	}
	records := make([]domain.MutationRecord, 0, len(rows))
	for _, row := range rows {
		record, decodeErr := decodeListRecoverableLoadMutation(row)
		if decodeErr != nil {
			return nil, fmt.Errorf("decode recoverable SQLite load mutation: %w", decodeErr)
		}
		records = append(records, record)
	}
	return records, nil
}

func (journal *loadMutationJournal) get(ctx context.Context, id string) (domain.MutationRecord, error) {
	row, err := journal.queries.GetLoadMutation(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.MutationRecord{}, fmt.Errorf("%w: load mutation", domain.ErrNotFound)
	}
	if err != nil {
		return domain.MutationRecord{}, fmt.Errorf("get SQLite load mutation: %w", err)
	}
	record, err := decodeGetLoadMutation(row)
	if err != nil {
		return domain.MutationRecord{}, fmt.Errorf("decode SQLite load mutation: %w", err)
	}
	return record, nil
}

func decodeGetLoadMutation(row sqlcgen.GetLoadMutationRow) (domain.MutationRecord, error) {
	return decodeLoadMutation(
		row.MutationID, row.ProjectID, row.Location, row.JobID,
		row.ConfigurationDigest, row.PlanFingerprint, row.DestinationJson, row.BeforeSchemaJson,
		row.Publication, row.Phase, row.InputFiles, row.InputBytes,
		row.OutputRows, row.CreatedDestination, row.UpdatedDestination,
	)
}

func decodeListRecoverableLoadMutation(row sqlcgen.ListRecoverableLoadMutationsRow) (domain.MutationRecord, error) {
	return decodeLoadMutation(
		row.MutationID, row.ProjectID, row.Location, row.JobID,
		row.ConfigurationDigest, row.PlanFingerprint, row.DestinationJson, row.BeforeSchemaJson,
		row.Publication, row.Phase, row.InputFiles, row.InputBytes,
		row.OutputRows, row.CreatedDestination, row.UpdatedDestination,
	)
}

func decodeLoadMutation(
	mutationID, projectID, location, jobID, configurationDigest, planFingerprint,
	destinationJSON, beforeSchemaJSON, publication, phase string,
	inputFiles, inputBytes int64,
	outputRows, createdDestination, updatedDestination sql.NullInt64,
) (domain.MutationRecord, error) {
	record := domain.MutationRecord{
		ID:                  mutationID,
		Job:                 domain.JobReference{ProjectID: projectID, Location: location, JobID: jobID},
		ConfigurationDigest: configurationDigest,
		PlanFingerprint:     planFingerprint,
		Publication:         domain.MutationPublication(publication),
		Phase:               domain.MutationPhase(phase),
		InputFiles:          inputFiles,
		InputBytes:          inputBytes,
	}
	if err := json.Unmarshal([]byte(destinationJSON), &record.Destination); err != nil {
		return domain.MutationRecord{}, fmt.Errorf("decode load mutation destination: %w", err)
	}
	if err := json.Unmarshal([]byte(beforeSchemaJSON), &record.BeforeSchema); err != nil {
		return domain.MutationRecord{}, fmt.Errorf("decode load mutation prior schema: %w", err)
	}
	if outputRows.Valid || createdDestination.Valid || updatedDestination.Valid {
		if !outputRows.Valid || !createdDestination.Valid || !updatedDestination.Valid {
			return domain.MutationRecord{}, errors.New("partial load mutation result")
		}
		record.Result = &domain.MutationResult{
			OutputRows: outputRows.Int64, CreatedDestination: createdDestination.Int64 == 1,
			UpdatedDestination: updatedDestination.Int64 == 1,
		}
	}
	if err := record.Validate(); err != nil {
		return domain.MutationRecord{}, fmt.Errorf("validate persisted load mutation: %w", err)
	}
	return record, nil
}

func encodeLoadMutationJSON(record domain.MutationRecord) (string, string, error) {
	destinationJSON, err := json.Marshal(record.Destination)
	if err != nil {
		return "", "", fmt.Errorf("encode load mutation destination: %w", err)
	}
	beforeSchemaJSON, err := json.Marshal(record.BeforeSchema)
	if err != nil {
		return "", "", fmt.Errorf("encode load mutation prior schema: %w", err)
	}
	return string(destinationJSON), string(beforeSchemaJSON), nil
}

func equivalentLoadMutationRecords(left, right domain.MutationRecord) bool {
	return reflect.DeepEqual(canonicalizeLoadMutationRecord(left), canonicalizeLoadMutationRecord(right))
}

func canonicalizeLoadMutationRecord(record domain.MutationRecord) domain.MutationRecord {
	record.Destination = domain.CloneTable(record.Destination)
	record.Destination.Schema = canonicalizeLoadMutationFields(record.Destination.Schema)
	if len(record.Destination.ClusteringFields) == 0 {
		record.Destination.ClusteringFields = nil
	}
	record.BeforeSchema = canonicalizeLoadMutationFields(record.BeforeSchema)
	return record
}

func canonicalizeLoadMutationFields(fields []domain.Field) []domain.Field {
	if len(fields) == 0 {
		return nil
	}
	result := make([]domain.Field, len(fields))
	for index, field := range fields {
		result[index] = field
		result[index].Fields = canonicalizeLoadMutationFields(field.Fields)
	}
	return result
}

func encodeBool(value bool) int64 {
	if value {
		return 1
	}
	return 0
}
