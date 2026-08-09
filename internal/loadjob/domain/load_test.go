package domain

import (
	"errors"
	"testing"
	"time"

	catalogdomain "github.com/leeyh0216/go-bemu/internal/domain"
)

func TestValidateConfigurationAcceptsOnlyGCSObjectURIs(t *testing.T) {
	valid := LoadConfiguration{
		SourceURIs: []string{"gs://bucket/path/*.parquet"},
		Destination: TableReference{
			ProjectID: "project", DatasetID: "dataset", TableID: "table",
		},
		SourceFormat:      FormatParquet,
		WriteDisposition:  WriteAppend,
		CreateDisposition: CreateIfNeeded,
	}
	if err := ValidateConfiguration(valid); err != nil {
		t.Fatalf("valid GCS source error = %v", err)
	}

	for name, source := range map[string]string{
		"empty":          "",
		"bare path":      "/tmp/input.parquet",
		"file":           "file:///tmp/input.parquet",
		"HTTP":           "https://storage.example/input.parquet",
		"missing bucket": "gs:///input.parquet",
		"missing object": "gs://bucket",
		"query":          "gs://bucket/input.parquet?generation=1",
		"fragment":       "gs://bucket/input.parquet#fragment",
	} {
		t.Run(name, func(t *testing.T) {
			configuration := valid
			configuration.SourceURIs = []string{source}
			if err := ValidateConfiguration(configuration); !errors.Is(err, ErrInvalid) {
				t.Fatalf("source %q error = %v", source, err)
			}
		})
	}
}

func TestConfigurationDigestIncludesParquetListInference(t *testing.T) {
	configuration := LoadConfiguration{
		SourceURIs:   []string{"gs://bucket/path/input.parquet"},
		Destination:  TableReference{ProjectID: "project", DatasetID: "dataset", TableID: "table"},
		SourceFormat: FormatParquet, WriteDisposition: WriteAppend, CreateDisposition: CreateIfNeeded,
	}
	withoutInference, err := ConfigurationDigest(configuration)
	if err != nil {
		t.Fatal(err)
	}
	configuration.ParquetOptions.EnableListInference = true
	withInference, err := ConfigurationDigest(configuration)
	if err != nil {
		t.Fatal(err)
	}
	if withInference == withoutInference {
		t.Fatal("Parquet list inference did not change load idempotency identity")
	}
}

func TestConfigurationNormalizesAndFingerprintsDestinationLayout(t *testing.T) {
	expiration := int64(86_400_000)
	configuration := LoadConfiguration{
		SourceURIs:   []string{"gs://bucket/path/input.parquet"},
		Destination:  TableReference{ProjectID: "project", DatasetID: "dataset", TableID: "table"},
		SourceFormat: FormatParquet, WriteDisposition: WriteAppend, CreateDisposition: CreateIfNeeded,
		Schema:           []Field{{Name: "event_time", Type: "TIMESTAMP"}, {Name: "customer_id", Type: "STRING"}},
		TimePartitioning: &TimePartitioning{Field: "event_time", ExpirationMs: &expiration},
		ClusteringFields: []string{"customer_id"},
	}
	job, err := NewJob(JobReference{ProjectID: "project", Location: "US", JobID: "layout"}, configuration, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if job.Configuration.TimePartitioning.Type != "DAY" {
		t.Fatalf("default partition type = %q", job.Configuration.TimePartitioning.Type)
	}
	changed := configuration
	changed.ClusteringFields = []string{"event_time"}
	changedDigest, err := ConfigurationDigest(normalizeConfiguration(changed))
	if err != nil {
		t.Fatal(err)
	}
	if changedDigest == job.ConfigurationDigest {
		t.Fatal("destination layout did not change load idempotency identity")
	}

	rangeConfiguration := configuration
	rangeConfiguration.TimePartitioning = nil
	rangeConfiguration.Schema = []Field{{Name: "bucket_id", Type: "INT64"}}
	rangeConfiguration.RangePartitioning = &RangePartitioning{
		Field: "bucket_id", Range: catalogdomain.Range{Start: 0, End: 100, Interval: 10},
	}
	rangeConfiguration.ClusteringFields = nil
	if _, err := NewJob(JobReference{ProjectID: "project", Location: "US", JobID: "range"}, rangeConfiguration, time.Unix(1, 0)); err != nil {
		t.Fatalf("range-partitioned configuration error = %v", err)
	}
	rangeConfiguration.TimePartitioning = &TimePartitioning{Type: "DAY"}
	if _, err := NewJob(JobReference{ProjectID: "project", Location: "US", JobID: "both"}, rangeConfiguration, time.Unix(1, 0)); !errors.Is(err, ErrInvalid) {
		t.Fatalf("mutually exclusive partitioning error = %v", err)
	}
}

func TestSchemaUpdateOptionsAreTypedUniqueAndAppendOnly(t *testing.T) {
	base := LoadConfiguration{
		SourceURIs:   []string{"gs://bucket/path/input.parquet"},
		Destination:  TableReference{ProjectID: "project", DatasetID: "dataset", TableID: "table"},
		SourceFormat: FormatParquet, WriteDisposition: WriteAppend, CreateDisposition: CreateIfNeeded,
	}
	base.SchemaUpdateOptions = []SchemaUpdateOption{"allow_field_addition", "ALLOW_FIELD_RELAXATION"}
	job, err := NewJob(JobReference{ProjectID: "project", Location: "US", JobID: "load"}, base, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if got := job.Configuration.SchemaUpdateOptions; len(got) != 2 || got[0] != AllowFieldAddition || got[1] != AllowFieldRelaxation {
		t.Fatalf("normalized options = %#v", got)
	}
	reversed := base
	reversed.SchemaUpdateOptions = []SchemaUpdateOption{AllowFieldRelaxation, AllowFieldAddition}
	reversedJob, err := NewJob(JobReference{ProjectID: "project", Location: "US", JobID: "load"}, reversed, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if reversedJob.ConfigurationDigest != job.ConfigurationDigest {
		t.Fatal("schema update option order changed configuration identity")
	}

	for name, mutate := range map[string]func(*LoadConfiguration){
		"unknown": func(configuration *LoadConfiguration) {
			configuration.SchemaUpdateOptions = []SchemaUpdateOption{"ALLOW_UNKNOWN"}
		},
		"duplicate": func(configuration *LoadConfiguration) {
			configuration.SchemaUpdateOptions = []SchemaUpdateOption{AllowFieldAddition, AllowFieldAddition}
		},
		"truncate": func(configuration *LoadConfiguration) {
			configuration.SchemaUpdateOptions = []SchemaUpdateOption{AllowFieldRelaxation}
			configuration.WriteDisposition = WriteTruncate
		},
	} {
		t.Run(name, func(t *testing.T) {
			configuration := base
			mutate(&configuration)
			if _, err := NewJob(JobReference{ProjectID: "project", Location: "US", JobID: name}, configuration, time.Unix(1, 0)); !errors.Is(err, ErrInvalid) {
				t.Fatalf("configuration error = %v", err)
			}
		})
	}
}
