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
	if config.MaxRowsPerResponse <= 0 || config.MaxSessions <= 0 {
		return fmt.Errorf("response row and session limits must be positive")
	}
	return nil
}
