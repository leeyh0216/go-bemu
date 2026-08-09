package domain

import (
	"errors"
	"testing"
)

func TestTableValidatesIngestionTimePartitionMetadata(t *testing.T) {
	base := Table{
		ProjectID: "test-project", DatasetID: "dataset", ID: "events",
		Schema: []Field{{Name: "id", Type: "INT64"}},
	}
	for _, typ := range []string{"DAY", "HOUR", "MONTH", "YEAR"} {
		table := base
		table.TimePartitioning = &TimePartitioning{Type: typ}
		if err := table.Validate(); err != nil {
			t.Fatalf("Validate(%s): %v", typ, err)
		}
		if got, ok := table.IngestionTimePartitioning(); !ok || got != typ {
			t.Fatalf("IngestionTimePartitioning(%s) = %q, %t", typ, got, ok)
		}
	}
}

func TestTableRejectsInvalidPartitionMetadataAndReservedFields(t *testing.T) {
	base := func() Table {
		return Table{
			ProjectID: "test-project", DatasetID: "dataset", ID: "events",
			Schema: []Field{{Name: "id", Type: "INT64"}, {Name: "event_date", Type: "DATE"}},
		}
	}
	tests := map[string]Table{
		"reserved time": func() Table { value := base(); value.Schema[0].Name = "_partitiontime"; return value }(),
		"reserved date": func() Table { value := base(); value.Schema[0].Name = "_PARTITIONDATE"; return value }(),
		"unknown type":  func() Table { value := base(); value.TimePartitioning = &TimePartitioning{Type: "WEEK"}; return value }(),
		"missing field": func() Table {
			value := base()
			value.TimePartitioning = &TimePartitioning{Type: "DAY", Field: "missing"}
			return value
		}(),
		"both modes": func() Table {
			value := base()
			value.TimePartitioning = &TimePartitioning{Type: "DAY"}
			value.RangePartitioning = &RangePartitioning{Field: "id", Range: Range{Start: 0, End: 10, Interval: 1}}
			return value
		}(),
	}
	for name, table := range tests {
		t.Run(name, func(t *testing.T) {
			if err := table.Validate(); !errors.Is(err, ErrInvalid) {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}
}

func TestTableValidatesClusteringFields(t *testing.T) {
	base := Table{
		ProjectID: "test-project", DatasetID: "dataset", ID: "events",
		Schema: []Field{
			{Name: "event_date", Type: "DATE"},
			{Name: "customer_id", Type: "STRING"},
			{Name: "items", Type: "INT64", Mode: "REPEATED"},
			{Name: "amount", Type: "FLOAT64"},
		},
	}
	valid := base
	valid.ClusteringFields = []string{"customer_id", "event_date"}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid clustering error = %v", err)
	}

	for name, fields := range map[string][]string{
		"empty":            {},
		"missing":          {"missing"},
		"duplicate":        {"customer_id", "CUSTOMER_ID"},
		"repeated":         {"items"},
		"unsupported type": {"amount"},
		"too many":         {"customer_id", "event_date", "customer_id_2", "customer_id_3", "customer_id_4"},
	} {
		t.Run(name, func(t *testing.T) {
			table := base
			table.ClusteringFields = fields
			if err := table.Validate(); !errors.Is(err, ErrInvalid) {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}
}
