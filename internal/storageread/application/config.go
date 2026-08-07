package application

// Operational values are injected rather than hidden in constructors. A
// file/environment adapter can populate this structure without changing the
// Storage Read core.

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"
)

type Config struct {
	Location             string
	ProtocolModelVersion string
	AllowedStreamCounts  []int32
	DefaultStreamCount   int32
	SessionTTL           time.Duration
	CleanupInterval      time.Duration
	MaxRowsPerResponse   int64
	MaxSessions          int
}

func validateConfig(config *Config) error {
	if !validResourceSegment(config.Location) {
		return fmt.Errorf("location must be a non-empty resource segment")
	}
	if strings.TrimSpace(config.ProtocolModelVersion) == "" {
		return fmt.Errorf("protocol model version is required")
	}
	if len(config.AllowedStreamCounts) == 0 {
		return fmt.Errorf("at least one allowed stream count is required")
	}
	config.AllowedStreamCounts = slices.Clone(config.AllowedStreamCounts)
	sort.Slice(config.AllowedStreamCounts, func(i, j int) bool {
		return config.AllowedStreamCounts[i] < config.AllowedStreamCounts[j]
	})
	for index, count := range config.AllowedStreamCounts {
		if count <= 0 {
			return fmt.Errorf("allowed stream counts must be positive")
		}
		if index > 0 && count == config.AllowedStreamCounts[index-1] {
			return fmt.Errorf("allowed stream counts must be unique")
		}
	}
	if !slices.Contains(config.AllowedStreamCounts, config.DefaultStreamCount) {
		return fmt.Errorf("default stream count must be allowed")
	}
	if config.SessionTTL <= 0 || config.CleanupInterval <= 0 {
		return fmt.Errorf("session TTL and cleanup interval must be positive")
	}
	if config.MaxRowsPerResponse <= 0 || config.MaxSessions <= 0 {
		return fmt.Errorf("response row and session limits must be positive")
	}
	return nil
}
