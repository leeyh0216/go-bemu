package domain

import (
	"errors"
	"testing"
	"time"

	catalogdomain "github.com/leeyh0216/go-bemu/internal/domain"
)

func TestResolvePartitionDecoratorUsesCanonicalTableLayout(t *testing.T) {
	base := TableReference{ProjectID: "project", DatasetID: "dataset", TableID: "events"}
	resolved, partitionID, decorated, err := SplitPartitionDecorator(TableReference{
		ProjectID: "project", DatasetID: "dataset", TableID: "events$2026080913",
	})
	if err != nil || !decorated || resolved != base || partitionID != "2026080913" {
		t.Fatalf("split = %+v %q %t %v", resolved, partitionID, decorated, err)
	}
	timeTarget, err := ResolvePartitionDecorator(partitionID, Table{
		Reference: base, Schema: []Field{{Name: "event_time", Type: "TIMESTAMP"}},
		TimePartitioning: &catalogdomain.TimePartitioning{Type: "HOUR", Field: "event_time"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if timeTarget.Kind != PartitionDecoratorTime ||
		!timeTarget.TimeStart.Equal(time.Date(2026, 8, 9, 13, 0, 0, 0, time.UTC)) ||
		!timeTarget.TimeEnd.Equal(time.Date(2026, 8, 9, 14, 0, 0, 0, time.UTC)) {
		t.Fatalf("time target = %+v", timeTarget)
	}

	rangeTarget, err := ResolvePartitionDecorator("40", Table{
		Reference: base, Schema: []Field{{Name: "bucket_id", Type: "INT64"}},
		RangePartitioning: &catalogdomain.RangePartitioning{
			Field: "bucket_id", Range: catalogdomain.Range{Start: 0, End: 100, Interval: 20},
		},
	})
	if err != nil || rangeTarget.Kind != PartitionDecoratorRange || rangeTarget.RangeStart != 40 || rangeTarget.RangeEnd != 60 {
		t.Fatalf("range target = %+v, %v", rangeTarget, err)
	}
	_, negativeID, decorated, err := SplitPartitionDecorator(TableReference{
		ProjectID: "project", DatasetID: "dataset", TableID: "events$-20",
	})
	if err != nil || !decorated || negativeID != "-20" {
		t.Fatalf("negative range split = %q %t %v", negativeID, decorated, err)
	}
}

func TestPartitionDecoratorRejectsInvalidOrUnpartitionedTargets(t *testing.T) {
	base := Table{Reference: TableReference{ProjectID: "project", DatasetID: "dataset", TableID: "events"}, Schema: []Field{{Name: "id", Type: "INT64"}}}
	for name, tableID := range map[string]string{
		"missing ID": "events$", "multiple": "events$2026$08", "non-decimal": "events$today",
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, _, err := SplitPartitionDecorator(TableReference{ProjectID: "project", DatasetID: "dataset", TableID: tableID}); !errors.Is(err, ErrInvalid) {
				t.Fatalf("SplitPartitionDecorator() error = %v", err)
			}
		})
	}
	if _, err := ResolvePartitionDecorator("20260809", base); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("unpartitioned error = %v", err)
	}
	base.RangePartitioning = &catalogdomain.RangePartitioning{
		Field: "id", Range: catalogdomain.Range{Start: 0, End: 100, Interval: 20},
	}
	if _, err := ResolvePartitionDecorator("30", base); !errors.Is(err, ErrInvalid) {
		t.Fatalf("range boundary error = %v", err)
	}
}
