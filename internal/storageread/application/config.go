package application

// Operational values are injected rather than hidden in constructors. A
// file/environment adapter can populate this structure without changing the
// Storage Read core.

import (
	"fmt"
	"strings"
	"time"
)

type Config struct {
	Location             string
	ProtocolModelVersion string
	// MaxStreams is the local ceiling applied to max_stream_count and
	// preferred_min_stream_count. Both request fields are advisory, so the
	// service may choose a smaller count.
	// Source: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#createreadsessionrequest
	MaxStreams         int32
	DefaultStreamCount int32
	SessionTTL         time.Duration
	CleanupInterval    time.Duration
	MaxRowsPerResponse int64
	MaxSessions        int
	// MaxSnapshotBytes is reserved before materialization. On success, the
	// reservation is reduced to SnapshotMetadata.RetainedBytes and held until
	// expiry/Close. This bounds concurrent materializers without coupling them
	// to the application budget implementation.
	MaxSnapshotBytes      int64
	MaxTotalSnapshotBytes int64
	// StateOperationTimeout bounds every lifecycle metadata call, including
	// startup reconciliation and shutdown transitions.
	StateOperationTimeout time.Duration
}

func validateConfig(config *Config) error {
	if !validResourceSegment(config.Location) {
		return fmt.Errorf("location must be a non-empty resource segment")
	}
	if strings.TrimSpace(config.ProtocolModelVersion) == "" {
		return fmt.Errorf("protocol model version is required")
	}
	if config.MaxStreams <= 0 || config.DefaultStreamCount <= 0 || config.DefaultStreamCount > config.MaxStreams {
		return fmt.Errorf("stream limit and default must be positive, and default must not exceed the limit")
	}
	if config.SessionTTL <= 0 || config.CleanupInterval <= 0 {
		return fmt.Errorf("session TTL and cleanup interval must be positive")
	}
	if config.MaxRowsPerResponse <= 0 || config.MaxSessions <= 0 || config.MaxSnapshotBytes <= 0 || config.MaxTotalSnapshotBytes <= 0 {
		return fmt.Errorf("response row, session, and snapshot byte limits must be positive")
	}
	if config.StateOperationTimeout <= 0 {
		return fmt.Errorf("state operation timeout must be positive")
	}
	if config.MaxSnapshotBytes > config.MaxTotalSnapshotBytes {
		return fmt.Errorf("per-session snapshot byte limit must not exceed the total snapshot byte limit")
	}
	return nil
}
