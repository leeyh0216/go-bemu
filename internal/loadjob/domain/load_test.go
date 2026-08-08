package domain

import (
	"errors"
	"testing"
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
