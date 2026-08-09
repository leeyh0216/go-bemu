package application

import (
	"context"
	"fmt"
	"time"

	"github.com/leeyh0216/go-bemu/internal/ports"
	"github.com/leeyh0216/go-bemu/internal/state"
)

// RecoverCatalogState reconciles durable PREPARED table changes and then
// checks the complete canonical catalog against physical storage. Callers must
// run it before opening public listeners or reporting readiness.
func (s *CatalogService) RecoverCatalogState(ctx context.Context) error {
	if err := s.resourceMutationMu.LockContext(ctx); err != nil {
		return err
	}
	defer s.resourceMutationMu.Unlock()

	if s.mutationJournal != nil {
		if err := s.recoverPendingTableChanges(ctx); err != nil {
			return err
		}
	}
	return s.validateCatalogStorage(ctx)
}

func (s *CatalogService) recoverPendingTableChanges(ctx context.Context) error {
	planner, ok := s.warehouse.(ports.TableSchemaPlanner)
	if !ok {
		return ddlUnsupported("configured engine cannot replan pending table schema changes")
	}
	mutator, ok := s.warehouse.(ports.TableSchemaMutator)
	if !ok {
		return ddlUnsupported("configured engine cannot inspect and recover pending table schema changes")
	}
	if provider, ok := s.warehouse.(ports.EngineCapabilityProvider); ok {
		capabilities := provider.EngineCapabilities().TableSchemaChanges
		if !capabilities.Transactional || !capabilities.InspectBeforeAfter {
			return ddlUnsupported("configured engine cannot safely recover pending table schema changes")
		}
	}

	for {
		pending, err := s.mutationJournal.ListPending(ctx, state.MaxPendingList)
		if err != nil {
			return fmt.Errorf("list pending canonical mutations: %w", err)
		}
		if len(pending) == 0 {
			return nil
		}
		for _, record := range pending {
			if err := s.recoverTableChange(ctx, planner, mutator, record); err != nil {
				return err
			}
		}
	}
}

func (s *CatalogService) recoverTableChange(
	ctx context.Context,
	planner ports.TableSchemaPlanner,
	mutator ports.TableSchemaMutator,
	record state.Mutation,
) error {
	change := record.TableChange
	plan, err := planner.PlanTableChange(change.Before, change.After)
	if err != nil {
		return fmt.Errorf("replan pending mutation %s: %w", record.ID, err)
	}
	if plan.BeforePhysicalFingerprint != record.BeforePhysicalFingerprint ||
		plan.AfterPhysicalFingerprint != record.AfterPhysicalFingerprint {
		return fmt.Errorf("pending mutation %s was planned by a different physical schema mapping", record.ID)
	}

	matchesAfter, err := mutator.TableSchemaMatches(ctx, change.After)
	if err != nil {
		return fmt.Errorf("inspect pending mutation %s after schema: %w", record.ID, err)
	}
	if matchesAfter {
		return s.commitRecoveredTableChange(ctx, record)
	}
	matchesBefore, err := mutator.TableSchemaMatches(ctx, change.Before)
	if err != nil {
		return fmt.Errorf("inspect pending mutation %s before schema: %w", record.ID, err)
	}
	if !matchesBefore {
		return fmt.Errorf("physical catalog drift for %s: schema matches neither side of pending mutation %s", record.ResourceKey, record.ID)
	}

	applyErr := mutator.ApplyTableSchemaChange(ctx, plan)
	if applyErr != nil {
		matchesAfter, afterErr := mutator.TableSchemaMatches(ctx, change.After)
		if afterErr != nil {
			return fmt.Errorf("inspect failed recovery mutation %s: %w", record.ID, afterErr)
		}
		if matchesAfter {
			return s.commitRecoveredTableChange(ctx, record)
		}
		matchesBefore, beforeErr := mutator.TableSchemaMatches(ctx, change.Before)
		if beforeErr != nil {
			return fmt.Errorf("inspect failed recovery mutation %s rollback: %w", record.ID, beforeErr)
		}
		if !matchesBefore {
			return fmt.Errorf("physical catalog drift for %s after failed mutation %s", record.ResourceKey, record.ID)
		}
		_, markErr := s.mutationJournal.MarkFailed(ctx, record.ID, state.Failure{
			Code: "physical.apply_failed", Digest: state.Fingerprint([]byte(applyErr.Error())),
		}, mutationCompletedAt(record, s.clock.Now()))
		if markErr != nil {
			return fmt.Errorf("record failed recovery mutation %s: %w", record.ID, markErr)
		}
		return nil
	}

	matchesAfter, err = mutator.TableSchemaMatches(ctx, change.After)
	if err != nil {
		return fmt.Errorf("verify recovered mutation %s: %w", record.ID, err)
	}
	if !matchesAfter {
		return fmt.Errorf("physical catalog drift for %s after applying mutation %s", record.ResourceKey, record.ID)
	}
	return s.commitRecoveredTableChange(ctx, record)
}

func (s *CatalogService) commitRecoveredTableChange(ctx context.Context, record state.Mutation) error {
	if _, err := s.mutationJournal.CommitTableChange(ctx, record.ID, mutationCompletedAt(record, s.clock.Now())); err != nil {
		return fmt.Errorf("publish recovered mutation %s: %w", record.ID, err)
	}
	return nil
}

func mutationCompletedAt(record state.Mutation, now time.Time) time.Time {
	if now.Before(record.PreparedAt) {
		return record.PreparedAt
	}
	return now
}

func (s *CatalogService) validateCatalogStorage(ctx context.Context) error {
	inspector, ok := s.warehouse.(ports.CatalogStorageInspector)
	if !ok {
		return ddlUnsupported("configured engine cannot validate canonical catalog storage")
	}
	snapshot := ports.CatalogStorageSnapshot{}
	projects, err := s.catalog.ListProjects(ctx)
	if err != nil {
		return fmt.Errorf("list canonical projects for storage validation: %w", err)
	}
	for _, project := range projects {
		datasets, err := s.catalog.ListDatasets(ctx, project.ID)
		if err != nil {
			return fmt.Errorf("list canonical datasets for storage validation: %w", err)
		}
		for _, dataset := range datasets {
			snapshot.Datasets = append(snapshot.Datasets, dataset)
			tables, err := s.catalog.ListTables(ctx, dataset.ProjectID, dataset.ID)
			if err != nil {
				return fmt.Errorf("list canonical tables for storage validation: %w", err)
			}
			snapshot.Tables = append(snapshot.Tables, tables...)
		}
	}
	if err := inspector.ValidateCatalogStorage(ctx, snapshot); err != nil {
		return fmt.Errorf("validate canonical catalog storage: %w", err)
	}
	return nil
}
