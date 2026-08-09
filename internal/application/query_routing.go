package application

// Query routing and generated-destination policy are isolated from job state
// transitions so backend/parser changes do not alter the REST lifecycle.
//
// Official behavior:
//   - omitted job locations are inferred from referenced/default/destination
//     datasets: https://cloud.google.com/bigquery/docs/locations#specify_locations
//   - all datasets read and written by a job must share a location:
//     https://cloud.google.com/bigquery/docs/locations#location_considerations
//   - queries without destinationTable receive an anonymous result table:
//     https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"

	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

func (s *QueryService) resolveQueryLocation(ctx context.Context, input QueryInput, analysis ports.QueryAnalysis) (string, error) {
	references := analysis.ReferencedTables
	type datasetReference struct {
		projectID string
		datasetID string
	}
	candidates := make([]datasetReference, 0, len(references)+2)
	seen := make(map[string]struct{}, len(references)+2)
	add := func(projectID, datasetID string) {
		if projectID == "" {
			projectID = input.ProjectID
		}
		if projectID == "" || datasetID == "" {
			return
		}
		key := projectID + "\x00" + datasetID
		if _, exists := seen[key]; exists {
			return
		}
		seen[key] = struct{}{}
		candidates = append(candidates, datasetReference{projectID: projectID, datasetID: datasetID})
	}
	for _, reference := range references {
		add(reference.ProjectID, reference.DatasetID)
	}
	if analysis.RequiresCatalogMutation {
		for _, target := range analysis.MutationTargets {
			add(target.ProjectID, target.DatasetID)
		}
	}
	add(input.DefaultProjectID, input.DefaultDataset)
	if input.Destination != nil {
		add(input.Destination.ProjectID, input.Destination.DatasetID)
	}

	if len(candidates) == 0 {
		if input.Location != "" {
			logQueryLocation(ctx, input.Location, false, 0, len(references))
			return input.Location, nil
		}
		logQueryLocation(ctx, s.defaultLocation, true, 0, len(references))
		return s.defaultLocation, nil
	}
	if s.destinations == nil {
		return "", fmt.Errorf("%w: dataset-aware query location requires destination catalog port", domain.ErrPrecondition)
	}
	// Resolve every structurally referenced table through the application
	// catalog, rather than querying DuckDB directly. This applies the same lazy
	// expiration/not-found contract used by tables.get and Storage Read before a
	// query can observe physical rows from an expired table.
	// https://cloud.google.com/bigquery/docs/managing-tables#table-expiration
	for _, reference := range references {
		if _, err := s.destinations.GetTable(ctx, reference.ProjectID, reference.DatasetID, reference.TableID); err != nil {
			return "", err
		}
	}
	for _, target := range analysis.MutationTargets {
		dataset, err := s.destinations.GetDataset(ctx, target.ProjectID, target.DatasetID)
		if err != nil {
			return "", err
		}
		if dataset.Hidden {
			return "", fmt.Errorf("%w: DML cannot target an anonymous cached-result table", domain.ErrInvalid)
		}
	}

	resolved := ""
	for _, candidate := range candidates {
		dataset, err := s.destinations.GetDataset(ctx, candidate.projectID, candidate.datasetID)
		if err != nil {
			return "", err
		}
		if dataset.Location == "" {
			return "", fmt.Errorf("%w: query dataset has no location metadata", domain.ErrPrecondition)
		}
		if resolved == "" {
			resolved = dataset.Location
			continue
		}
		if !strings.EqualFold(resolved, dataset.Location) {
			return "", fmt.Errorf("%w: query references datasets in different locations; capability=%s", domain.ErrInvalid, domain.CapabilityQueryDatasetLocationV1)
		}
	}
	if input.Location != "" && !strings.EqualFold(input.Location, resolved) {
		return "", fmt.Errorf("%w: requested query location differs from referenced dataset location; capability=%s", domain.ErrInvalid, domain.CapabilityQueryDatasetLocationV1)
	}
	if input.Location != "" {
		logQueryLocation(ctx, input.Location, false, len(candidates), len(references))
		return input.Location, nil
	}
	logQueryLocation(ctx, resolved, true, len(candidates), len(references))
	return resolved, nil
}

func logQueryLocation(ctx context.Context, location string, inferred bool, datasetCount, referenceCount int) {
	slog.InfoContext(ctx, "query location resolved",
		"event", "application.query.location.resolved", "capability", domain.CapabilityQueryDatasetLocationV1,
		"location", location, "inferred", inferred, "dataset_count", datasetCount,
		"referenced_table_count", referenceCount)
}

func anonymousQueryDestination(projectID, location, jobID string) domain.TableReference {
	// Full SHA-256 names avoid leaking request/job content and make the generated
	// identity deterministic before JobRepository insertion. A user-created
	// collision is rejected by EnsureAnonymousDataset instead of overwritten.
	locationFingerprint := queryRoutingFingerprint(strings.ToUpper(location))
	tableFingerprint := queryRoutingFingerprint(projectID, strings.ToUpper(location), jobID)
	return domain.TableReference{
		ProjectID: projectID,
		DatasetID: "_bqemu_anonymous_" + locationFingerprint,
		TableID:   "_bqemu_query_" + tableFingerprint,
	}
}

func queryRoutingFingerprint(parts ...string) string {
	digest := sha256.New()
	for index, part := range parts {
		if index != 0 {
			digest.Write([]byte{0})
		}
		digest.Write([]byte(part))
	}
	return hex.EncodeToString(digest.Sum(nil))
}
