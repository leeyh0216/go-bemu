package application

// Operational limits are injected so deployments can bound memory without
// changing the official Storage Write request contract. ProtoData and envelope
// budgets must fit within the complete 20 MB AppendRowsRequest limit.
// https://cloud.google.com/bigquery/quotas#write-api-limits

import (
	"fmt"
	"strings"
	"time"
)

const ProtocolMaxAppendRequestBytes = 20 * 1024 * 1024

type Config struct {
	Location                    string
	ProtocolModelVersion        string
	MaxStreams                  int
	MaxAppendBytes              int
	MaxAppendEnvelopeBytes      int
	MaxConcurrentAppendRequests int
	OrphanTTL                   time.Duration
	CleanupInterval             time.Duration
}

func validateConfig(config Config) error {
	if strings.TrimSpace(config.Location) == "" || strings.Contains(config.Location, "/") {
		return fmt.Errorf("location must be a non-empty resource segment")
	}
	if strings.TrimSpace(config.ProtocolModelVersion) == "" {
		return fmt.Errorf("protocol model version is required")
	}
	if config.MaxStreams <= 0 {
		return fmt.Errorf("maximum logical stream count must be positive")
	}
	if config.MaxAppendBytes <= 0 {
		return fmt.Errorf("maximum append bytes must be positive")
	}
	if config.MaxAppendEnvelopeBytes <= 0 {
		return fmt.Errorf("maximum append envelope bytes must be positive")
	}
	if config.MaxAppendBytes > ProtocolMaxAppendRequestBytes-config.MaxAppendEnvelopeBytes {
		return fmt.Errorf("append payload and envelope budgets must fit within the maximum request size %d", ProtocolMaxAppendRequestBytes)
	}
	if config.MaxConcurrentAppendRequests <= 0 {
		return fmt.Errorf("maximum concurrent append requests must be positive")
	}
	if config.OrphanTTL <= 0 || config.CleanupInterval <= 0 {
		return fmt.Errorf("orphan TTL and cleanup interval must be positive")
	}
	return nil
}
