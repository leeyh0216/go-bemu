package tabledata

import (
	"errors"
	"testing"
)

func TestAccumulatorIsDeterministicAndStopsBeforeSecondRowCrossesPageBudget(t *testing.T) {
	first := []any{int64(1), map[string]any{"z": "last", "a": "first"}}
	probe := NewAccumulator(0)
	if included, err := probe.Add(first, 1_000); err != nil || !included {
		t.Fatalf("probe add = included %v, error %v", included, err)
	}
	oneRowBytes := probe.Metrics().Bytes

	limited := NewAccumulator(oneRowBytes)
	if included, err := limited.Add(first, 1_000); err != nil || !included {
		t.Fatalf("first limited add = included %v, error %v", included, err)
	}
	if included, err := limited.Add([]any{int64(2), "second"}, 1_000); err != nil || included {
		t.Fatalf("second limited add = included %v, error %v, want clean page boundary", included, err)
	}

	reordered := NewAccumulator(0)
	if included, err := reordered.Add([]any{int64(1), map[string]any{"a": "first", "z": "last"}}, 1_000); err != nil || !included {
		t.Fatalf("reordered add = included %v, error %v", included, err)
	}
	if reordered.Metrics() != probe.Metrics() {
		t.Fatalf("canonical metrics differ by map insertion order: %#v != %#v", reordered.Metrics(), probe.Metrics())
	}
}

func TestAccumulatorAllowsOnlyOneRowPageExceptionAndRejectsRowHardLimit(t *testing.T) {
	row := []any{"a value larger than the normal page budget"}
	accumulator := NewAccumulator(1)
	if included, err := accumulator.Add(row, 1_000); err != nil || !included {
		t.Fatalf("single-row exception = included %v, error %v", included, err)
	}
	if included, err := accumulator.Add([]any{"second"}, 1_000); err != nil || included {
		t.Fatalf("second row after exception = included %v, error %v", included, err)
	}

	tooSmall := NewAccumulator(1_000)
	if included, err := tooSmall.Add(row, 8); included || !errors.Is(err, ErrRowTooLarge) {
		t.Fatalf("hard row limit = included %v, error %v", included, err)
	}
}
