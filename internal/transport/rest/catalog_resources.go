package rest

// Official REST resource representations:
//   - https://cloud.google.com/bigquery/docs/reference/rest/v2/datasets#Dataset
//   - https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#Table
//   - https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TableFieldSchema
//
// BigQuery's JSON mapping represents int64 metadata, including timestamps and
// partition expiration, as decimal strings. These transport DTOs preserve that
// wire contract and translate only at the domain boundary.

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

type projectReference struct {
	ProjectID string `json:"projectId"`
}

type projectResource struct {
	Kind             string           `json:"kind,omitempty"`
	ID               string           `json:"id,omitempty"`
	FriendlyName     string           `json:"friendlyName,omitempty"`
	Description      string           `json:"description,omitempty"`
	ProjectReference projectReference `json:"projectReference"`
	CreationTime     string           `json:"creationTime,omitempty"`
}

type projectCreateRequest struct {
	ProjectID    string `json:"projectId"`
	FriendlyName string `json:"friendlyName"`
	Description  string `json:"description"`
}

type datasetReference struct {
	ProjectID string `json:"projectId,omitempty"`
	DatasetID string `json:"datasetId"`
}

type datasetResource struct {
	Kind                         string            `json:"kind,omitempty"`
	ETag                         string            `json:"etag,omitempty"`
	ID                           string            `json:"id,omitempty"`
	SelfLink                     string            `json:"selfLink,omitempty"`
	DatasetReference             datasetReference  `json:"datasetReference"`
	FriendlyName                 string            `json:"friendlyName,omitempty"`
	Description                  string            `json:"description,omitempty"`
	Location                     string            `json:"location,omitempty"`
	Labels                       map[string]string `json:"labels,omitempty"`
	DefaultTableExpirationMs     string            `json:"defaultTableExpirationMs,omitempty"`
	DefaultPartitionExpirationMs string            `json:"defaultPartitionExpirationMs,omitempty"`
	CreationTime                 string            `json:"creationTime,omitempty"`
	LastModifiedTime             string            `json:"lastModifiedTime,omitempty"`
}

type tableReference struct {
	ProjectID string `json:"projectId,omitempty"`
	DatasetID string `json:"datasetId,omitempty"`
	TableID   string `json:"tableId"`
}

type tableFieldSchema struct {
	Name         string              `json:"name"`
	Type         string              `json:"type"`
	Mode         string              `json:"mode,omitempty"`
	Description  string              `json:"description,omitempty"`
	Precision    *int64              `json:"precision,omitempty,string"`
	Scale        *int64              `json:"scale,omitempty,string"`
	RoundingMode domain.RoundingMode `json:"roundingMode,omitempty"`
	Fields       []tableFieldSchema  `json:"fields,omitempty"`
}

// UnmarshalJSON accepts both representations emitted by BigQuery REST clients
// for TableFieldSchema's int64 decimal metadata. Responses retain the official
// string representation through the fields' json ",string" tags above.
func (field *tableFieldSchema) UnmarshalJSON(payload []byte) error {
	var wire struct {
		Name         string              `json:"name"`
		Type         string              `json:"type"`
		Mode         string              `json:"mode,omitempty"`
		Description  string              `json:"description,omitempty"`
		Precision    json.RawMessage     `json:"precision"`
		Scale        json.RawMessage     `json:"scale"`
		RoundingMode domain.RoundingMode `json:"roundingMode,omitempty"`
		Fields       []tableFieldSchema  `json:"fields,omitempty"`
	}
	if err := json.Unmarshal(payload, &wire); err != nil {
		return err
	}
	precision, err := parseTableFieldInt64(wire.Precision)
	if err != nil {
		return fmt.Errorf("precision: %w", err)
	}
	scale, err := parseTableFieldInt64(wire.Scale)
	if err != nil {
		return fmt.Errorf("scale: %w", err)
	}
	*field = tableFieldSchema{
		Name:         wire.Name,
		Type:         wire.Type,
		Mode:         wire.Mode,
		Description:  wire.Description,
		Precision:    precision,
		Scale:        scale,
		RoundingMode: wire.RoundingMode,
		Fields:       wire.Fields,
	}
	return nil
}

func parseTableFieldInt64(payload json.RawMessage) (*int64, error) {
	if len(payload) == 0 || string(payload) == "null" {
		return nil, nil
	}
	var numeric int64
	if err := json.Unmarshal(payload, &numeric); err == nil {
		return &numeric, nil
	}
	var text string
	if err := json.Unmarshal(payload, &text); err != nil {
		return nil, fmt.Errorf("must be an int64 JSON number or decimal string")
	}
	parsed, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("must be an int64 decimal string: %w", err)
	}
	return &parsed, nil
}

type tableSchema struct {
	Fields []tableFieldSchema `json:"fields"`
}

type timePartitioningResource struct {
	Type         string `json:"type,omitempty"`
	Field        string `json:"field,omitempty"`
	ExpirationMs string `json:"expirationMs,omitempty"`
}

type rangeResource struct {
	Start    string `json:"start"`
	End      string `json:"end"`
	Interval string `json:"interval"`
}

type rangePartitioningResource struct {
	Field string        `json:"field"`
	Range rangeResource `json:"range"`
}

type clusteringResource struct {
	Fields []string `json:"fields"`
}

type viewResource struct {
	Query        string `json:"query"`
	UseLegacySQL bool   `json:"useLegacySql"`
}

type tableResource struct {
	Kind                     string                     `json:"kind,omitempty"`
	ETag                     string                     `json:"etag,omitempty"`
	ID                       string                     `json:"id,omitempty"`
	SelfLink                 string                     `json:"selfLink,omitempty"`
	TableReference           tableReference             `json:"tableReference"`
	FriendlyName             string                     `json:"friendlyName,omitempty"`
	Description              string                     `json:"description,omitempty"`
	Labels                   map[string]string          `json:"labels,omitempty"`
	Type                     string                     `json:"type,omitempty"`
	Schema                   tableSchema                `json:"schema"`
	Location                 string                     `json:"location,omitempty"`
	ExpirationTime           string                     `json:"expirationTime,omitempty"`
	TimePartitioning         *timePartitioningResource  `json:"timePartitioning,omitempty"`
	RangePartitioning        *rangePartitioningResource `json:"rangePartitioning,omitempty"`
	Clustering               *clusteringResource        `json:"clustering,omitempty"`
	DefaultRoundingMode      *domain.RoundingMode       `json:"defaultRoundingMode,omitempty"`
	View                     *viewResource              `json:"view,omitempty"`
	CreationTime             string                     `json:"creationTime,omitempty"`
	LastModifiedTime         string                     `json:"lastModifiedTime,omitempty"`
	NumBytes                 string                     `json:"numBytes,omitempty"`
	NumLongTermBytes         string                     `json:"numLongTermBytes,omitempty"`
	NumRows                  string                     `json:"numRows,omitempty"`
	NumActiveLogicalBytes    string                     `json:"numActiveLogicalBytes,omitempty"`
	NumLongTermLogicalBytes  string                     `json:"numLongTermLogicalBytes,omitempty"`
	NumTotalLogicalBytes     string                     `json:"numTotalLogicalBytes,omitempty"`
	NumActivePhysicalBytes   string                     `json:"numActivePhysicalBytes,omitempty"`
	NumLongTermPhysicalBytes string                     `json:"numLongTermPhysicalBytes,omitempty"`
	NumTotalPhysicalBytes    string                     `json:"numTotalPhysicalBytes,omitempty"`
}

func tableFromLogicalView(view domain.View, baseURL string) tableResource {
	resource := tableFromDomain(domain.Table{
		ProjectID: view.ProjectID, DatasetID: view.DatasetID, ID: view.ID,
		FriendlyName: view.FriendlyName, Description: view.Description, Labels: view.Labels,
		Type: "VIEW", Schema: view.Schema, Location: view.Location,
		CreatedAt: view.CreatedAt, UpdatedAt: view.UpdatedAt,
	}, baseURL)
	resource.View = &viewResource{Query: view.Query, UseLegacySQL: view.UseLegacySQL}
	return resource
}

func projectFromDomain(project domain.Project) projectResource {
	return projectResource{
		Kind:             "bigquery#project",
		ID:               project.ID,
		FriendlyName:     project.FriendlyName,
		Description:      project.Description,
		ProjectReference: projectReference{ProjectID: project.ID},
		CreationTime:     millis(project.CreatedAt),
	}
}

func datasetFromDomain(dataset domain.Dataset, baseURL string) datasetResource {
	resource := datasetResource{
		Kind:             "bigquery#dataset",
		ETag:             metadataETag(dataset),
		ID:               dataset.ProjectID + ":" + dataset.ID,
		SelfLink:         baseURL + "/bigquery/v2/projects/" + dataset.ProjectID + "/datasets/" + dataset.ID,
		DatasetReference: datasetReference{ProjectID: dataset.ProjectID, DatasetID: dataset.ID},
		FriendlyName:     dataset.FriendlyName,
		Description:      dataset.Description,
		Location:         dataset.Location,
		Labels:           dataset.Labels,
		CreationTime:     millis(dataset.CreatedAt),
		LastModifiedTime: millis(dataset.UpdatedAt),
	}
	if dataset.DefaultTableExpirationMs != nil {
		resource.DefaultTableExpirationMs = strconv.FormatInt(*dataset.DefaultTableExpirationMs, 10)
	}
	if dataset.DefaultPartitionExpirationMs != nil {
		resource.DefaultPartitionExpirationMs = strconv.FormatInt(*dataset.DefaultPartitionExpirationMs, 10)
	}
	return resource
}

func tableFromDomain(table domain.Table, baseURL string) tableResource {
	resource := tableResource{
		Kind:             "bigquery#table",
		ETag:             metadataETag(table),
		ID:               fmt.Sprintf("%s:%s.%s", table.ProjectID, table.DatasetID, table.ID),
		SelfLink:         fmt.Sprintf("%s/bigquery/v2/projects/%s/datasets/%s/tables/%s", baseURL, table.ProjectID, table.DatasetID, table.ID),
		TableReference:   tableReference{ProjectID: table.ProjectID, DatasetID: table.DatasetID, TableID: table.ID},
		FriendlyName:     table.FriendlyName,
		Description:      table.Description,
		Labels:           table.Labels,
		Type:             table.Type,
		Schema:           tableSchema{Fields: fieldsFromDomain(table.Schema)},
		Location:         table.Location,
		CreationTime:     millis(table.CreatedAt),
		LastModifiedTime: millis(table.UpdatedAt),
	}
	if table.ExpirationTime != nil {
		resource.ExpirationTime = millis(*table.ExpirationTime)
	}
	if table.TimePartitioning != nil {
		resource.TimePartitioning = &timePartitioningResource{
			Type: table.TimePartitioning.Type, Field: table.TimePartitioning.Field,
			ExpirationMs: strconv.FormatInt(table.TimePartitioning.ExpirationMs, 10),
		}
	}
	if table.RangePartitioning != nil {
		resource.RangePartitioning = &rangePartitioningResource{
			Field: table.RangePartitioning.Field,
			Range: rangeResource{
				Start:    strconv.FormatInt(table.RangePartitioning.Range.Start, 10),
				End:      strconv.FormatInt(table.RangePartitioning.Range.End, 10),
				Interval: strconv.FormatInt(table.RangePartitioning.Range.Interval, 10),
			},
		}
	}
	if len(table.ClusteringFields) > 0 {
		resource.Clustering = &clusteringResource{Fields: append([]string(nil), table.ClusteringFields...)}
	}
	return resource
}

func metadataETag(value any) string {
	encoded, _ := json.Marshal(value)
	digest := sha256.Sum256(encoded)
	return base64.RawURLEncoding.EncodeToString(digest[:])
}

func fieldsFromDomain(fields []domain.Field) []tableFieldSchema {
	out := make([]tableFieldSchema, len(fields))
	for i, field := range fields {
		out[i] = tableFieldSchema{
			Name: field.Name, Type: field.Type, Mode: field.Mode,
			Description: field.Description, Precision: field.Precision, Scale: field.Scale, RoundingMode: field.RoundingMode,
			Fields: fieldsFromDomain(field.Fields),
		}
		if out[i].Mode == "" {
			out[i].Mode = "NULLABLE"
		}
	}
	return out
}

func fieldsToDomain(fields []tableFieldSchema) []domain.Field {
	out := make([]domain.Field, len(fields))
	for i, field := range fields {
		out[i] = domain.Field{
			Name: field.Name, Type: field.Type, Mode: field.Mode,
			Description: field.Description, Precision: field.Precision, Scale: field.Scale, RoundingMode: field.RoundingMode,
			Fields: fieldsToDomain(field.Fields),
		}
	}
	return out
}

func millis(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return strconv.FormatInt(value.UnixMilli(), 10)
}
