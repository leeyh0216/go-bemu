package application

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
	"github.com/leeyh0216/go-bemu/internal/querylang/semantic"
)

var _ ports.ViewDDLExecutor = (*CatalogService)(nil)

func (s *CatalogService) ExecuteViewDDL(
	ctx context.Context,
	statement semantic.Statement,
	sourceSQL string,
	_ string,
) error {
	if s.views == nil || s.viewStorage == nil {
		return unsupportedDDL("logical view metadata or storage is not configured")
	}
	if err := s.resourceMutationMu.LockContext(ctx); err != nil {
		return err
	}
	defer s.resourceMutationMu.Unlock()
	switch syntax := statement.Syntax().(type) {
	case *queryast.CreateViewStatement:
		return s.createViewLocked(ctx, statement, syntax, sourceSQL)
	case *queryast.DropViewStatement:
		return s.dropViewLocked(ctx, statement, syntax)
	default:
		return unsupportedDDL("analyzed statement is not logical view DDL")
	}
}

func (s *CatalogService) createViewLocked(
	ctx context.Context,
	statement semantic.Statement,
	syntax *queryast.CreateViewStatement,
	sourceSQL string,
) error {
	target, err := statement.RequireRelationBinding(syntax.Target().NodeKey())
	if err != nil {
		return err
	}
	reference, physical := target.Reference()
	if !physical {
		return invalidAnalyzedDDL()
	}
	dataset, err := s.catalog.GetDataset(ctx, reference.ProjectID, reference.DatasetID)
	if err != nil {
		return err
	}
	querySQL, err := viewQuerySource(syntax, sourceSQL)
	if err != nil {
		return err
	}
	schema, err := viewSchema(statement.OutputColumns())
	if err != nil {
		return err
	}
	dependencies := viewDependencies(statement.ReferencedTables(), reference)
	if err := s.validateViewDependencies(ctx, reference, dependencies); err != nil {
		return err
	}
	if err := s.ensureAcyclicViewDependencies(ctx, reference, dependencies); err != nil {
		return err
	}
	now := s.clock.Now()
	view := domain.View{
		ProjectID: reference.ProjectID, DatasetID: reference.DatasetID, ID: reference.TableID,
		Query: querySQL, UseLegacySQL: false, Schema: schema, Dependencies: dependencies,
		AnalysisFingerprint: statement.AnalysisFingerprint(), Location: dataset.Location,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := view.Validate(); err != nil {
		return err
	}
	if table, tableErr := s.catalog.GetTable(ctx, reference.ProjectID, reference.DatasetID, reference.TableID); tableErr == nil {
		_ = table
		return fmt.Errorf("%w: table %s/%s/%s conflicts with view", domain.ErrConflict, reference.ProjectID, reference.DatasetID, reference.TableID)
	} else if !errors.Is(tableErr, domain.ErrNotFound) {
		return tableErr
	}
	existing, existingErr := s.views.GetView(ctx, reference.ProjectID, reference.DatasetID, reference.TableID)
	if existingErr == nil {
		if !syntax.OrReplace() {
			return fmt.Errorf("%w: view %s/%s/%s", domain.ErrConflict, reference.ProjectID, reference.DatasetID, reference.TableID)
		}
		view.CreatedAt = existing.CreatedAt
	} else if !errors.Is(existingErr, domain.ErrNotFound) {
		return existingErr
	}
	if err := s.viewStorage.CreateLogicalView(ctx, statement, reference, syntax.OrReplace()); err != nil {
		return err
	}
	if existingErr == nil {
		return s.views.ReplaceView(ctx, view)
	}
	if err := s.views.CreateView(ctx, view); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.compensationTimeout)
		cleanupErr := s.viewStorage.DropLogicalView(cleanupCtx, reference)
		cancel()
		if cleanupErr != nil {
			return errors.Join(err, fmt.Errorf("compensate unpublished logical view: %w", cleanupErr))
		}
		return err
	}
	return nil
}

func (s *CatalogService) dropViewLocked(ctx context.Context, statement semantic.Statement, syntax *queryast.DropViewStatement) error {
	binding, err := statement.RequireRelationBinding(syntax.Target().NodeKey())
	if err != nil {
		return err
	}
	reference, physical := binding.Reference()
	if !physical {
		return invalidAnalyzedDDL()
	}
	if _, err := s.views.GetView(ctx, reference.ProjectID, reference.DatasetID, reference.TableID); err != nil {
		return err
	}
	if err := s.ensureNoDependentViews(ctx, reference); err != nil {
		return err
	}
	if err := s.viewStorage.DropLogicalView(ctx, reference); err != nil {
		return err
	}
	return s.views.DeleteView(ctx, reference.ProjectID, reference.DatasetID, reference.TableID)
}

func viewQuerySource(syntax *queryast.CreateViewStatement, source string) (string, error) {
	span := syntax.QuerySource().Span()
	if span.Start() < 0 || span.End() < span.Start() || span.End() > len(source) {
		return "", invalidAnalyzedDDL()
	}
	query := source[span.Start():span.End()]
	if strings.TrimSpace(query) == "" {
		return "", invalidAnalyzedDDL()
	}
	return query, nil
}

func viewDependencies(references []domain.TableReference, target domain.TableReference) []domain.TableReference {
	seen := make(map[string]struct{}, len(references))
	dependencies := make([]domain.TableReference, 0, len(references))
	for _, reference := range references {
		if reference == target {
			continue
		}
		key := strings.ToLower(reference.ProjectID + "\x00" + reference.DatasetID + "\x00" + reference.TableID)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		dependencies = append(dependencies, reference)
	}
	return dependencies
}

func (s *CatalogService) validateViewDependencies(ctx context.Context, target domain.TableReference, dependencies []domain.TableReference) error {
	for _, dependency := range dependencies {
		if dependency == target {
			return fmt.Errorf("%w: logical view cycle references itself", domain.ErrInvalid)
		}
		if _, err := s.catalog.GetTable(ctx, dependency.ProjectID, dependency.DatasetID, dependency.TableID); err == nil {
			continue
		} else if !errors.Is(err, domain.ErrNotFound) {
			return err
		}
		if _, err := s.views.GetView(ctx, dependency.ProjectID, dependency.DatasetID, dependency.TableID); err != nil {
			return err
		}
	}
	return nil
}

// ensureAcyclicViewDependencies verifies the candidate before either DuckDB or
// metadata is changed. View dependencies are persisted canonical references,
// so this is graph traversal rather than SQL-text inspection.
func (s *CatalogService) ensureAcyclicViewDependencies(ctx context.Context, target domain.TableReference, dependencies []domain.TableReference) error {
	const maximumViewDependencyDepth = 64
	visiting := make(map[string]bool)
	visited := make(map[string]bool)
	var visit func(domain.TableReference, int) error
	visit = func(reference domain.TableReference, depth int) error {
		if reference == target {
			return fmt.Errorf("%w: logical view dependency cycle", domain.ErrInvalid)
		}
		if depth > maximumViewDependencyDepth {
			return fmt.Errorf("%w: logical view dependency depth exceeds %d", domain.ErrPrecondition, maximumViewDependencyDepth)
		}
		key := strings.ToLower(reference.ProjectID + "\x00" + reference.DatasetID + "\x00" + reference.TableID)
		if visited[key] {
			return nil
		}
		if visiting[key] {
			return fmt.Errorf("%w: logical view dependency cycle", domain.ErrInvalid)
		}
		view, err := s.views.GetView(ctx, reference.ProjectID, reference.DatasetID, reference.TableID)
		if errors.Is(err, domain.ErrNotFound) {
			// A physical table terminates the view graph.
			return nil
		}
		if err != nil {
			return err
		}
		visiting[key] = true
		defer delete(visiting, key)
		for _, dependency := range view.Dependencies {
			if err := visit(dependency, depth+1); err != nil {
				return err
			}
		}
		visited[key] = true
		return nil
	}
	for _, dependency := range dependencies {
		if err := visit(dependency, 1); err != nil {
			return err
		}
	}
	return nil
}

func (s *CatalogService) ensureNoDependentViews(ctx context.Context, target domain.TableReference) error {
	if s.views == nil {
		return nil
	}
	datasets, err := s.catalog.ListDatasets(ctx, target.ProjectID)
	if err != nil {
		return err
	}
	for _, dataset := range datasets {
		views, err := s.views.ListViews(ctx, target.ProjectID, dataset.ID)
		if err != nil {
			return err
		}
		for _, view := range views {
			for _, dependency := range view.Dependencies {
				if dependency == target {
					return fmt.Errorf("%w: view %s/%s/%s has dependent view %s/%s/%s", domain.ErrConflict,
						target.ProjectID, target.DatasetID, target.TableID, view.ProjectID, view.DatasetID, view.ID)
				}
			}
		}
	}
	return nil
}

func viewSchema(columns []semantic.Column) ([]domain.Field, error) {
	if len(columns) == 0 {
		return nil, fmt.Errorf("%w: logical view requires an analyzed output schema", domain.ErrPrecondition)
	}
	fields := make([]domain.Field, len(columns))
	for index, column := range columns {
		field, err := viewField(column.Name(), column.Type())
		if err != nil {
			return nil, err
		}
		fields[index] = field
	}
	return fields, nil
}

func viewField(name string, typ semantic.Type) (domain.Field, error) {
	field := domain.Field{Name: name, Mode: "NULLABLE"}
	switch typ.Kind() {
	case semantic.TypeBool:
		field.Type = "BOOLEAN"
	case semantic.TypeInt64:
		field.Type = "INTEGER"
	case semantic.TypeFloat64:
		field.Type = "FLOAT"
	case semantic.TypeNumeric, semantic.TypeBigNumeric:
		field.Type = string(typ.Kind())
		if value, present := typ.Precision(); present {
			field.Precision = domain.CloneOptionalInt64(&value)
		}
		if value, present := typ.Scale(); present {
			field.Scale = domain.CloneOptionalInt64(&value)
		}
		field.RoundingMode = typ.RoundingMode()
	case semantic.TypeString:
		field.Type = "STRING"
	case semantic.TypeBytes:
		field.Type = "BYTES"
	case semantic.TypeDate:
		field.Type = "DATE"
	case semantic.TypeDatetime:
		field.Type = "DATETIME"
	case semantic.TypeTime:
		field.Type = "TIME"
	case semantic.TypeTimestamp:
		field.Type = "TIMESTAMP"
	case semantic.TypeJSON:
		field.Type = "JSON"
	case semantic.TypeArray:
		element, ok := typ.Element()
		if !ok || element.Kind() == semantic.TypeArray {
			return domain.Field{}, fmt.Errorf("%w: nested array view output is unsupported", domain.ErrUnsupported)
		}
		var err error
		field, err = viewField(name, element)
		if err != nil {
			return domain.Field{}, err
		}
		field.Mode = "REPEATED"
	case semantic.TypeStruct:
		field.Type = "RECORD"
		children := typ.Fields()
		field.Fields = make([]domain.Field, len(children))
		for index, child := range children {
			converted, err := viewField(child.Name(), child.Type())
			if err != nil {
				return domain.Field{}, err
			}
			field.Fields[index] = converted
		}
	default:
		return domain.Field{}, fmt.Errorf("%w: view output type is unsupported", domain.ErrUnsupported)
	}
	if err := field.Validate(); err != nil {
		return domain.Field{}, err
	}
	return field, nil
}
