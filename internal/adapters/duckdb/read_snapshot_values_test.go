package duckdb

import (
	"math"
	"math/big"
	"testing"
)

func TestSnapshotInt64AcceptsBoundedDriverIntegers(t *testing.T) {
	for _, value := range []int64{math.MinInt64, 0, math.MaxInt64} {
		observed, ok := snapshotInt64(*big.NewInt(value))
		if !ok || observed != value {
			t.Fatalf("snapshotInt64(%d) = (%d, %t)", value, observed, ok)
		}
	}
	overflow := new(big.Int).Add(new(big.Int).SetUint64(math.MaxInt64), big.NewInt(1))
	if observed, ok := snapshotInt64(*overflow); ok {
		t.Fatalf("snapshotInt64(overflow) = (%d, true)", observed)
	}
}
