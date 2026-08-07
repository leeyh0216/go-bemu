package application

// Operational limits are injected so connector matrices can exercise stream
// negotiation without recompiling the emulator. The BigQuery Storage client
// 3.22.1 used by spark-bigquery-connector 0.44.2 measures ProtoData against a
// 20 MiB client maximum; the connector batches at 95 percent of that value.
// https://repo.maven.apache.org/maven2/com/google/cloud/google-cloud-bigquerystorage/3.22.1/google-cloud-bigquerystorage-3.22.1-sources.jar
// https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/0.44.2/bigquery-connector-common/src/main/java/com/google/cloud/bigquery/connector/common/BigQueryDirectDataWriterHelper.java#L50-L55

import (
	"fmt"
	"strings"
	"time"
)

const ProtocolMaxAppendBytes = 20 * 1024 * 1024

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
	if config.MaxAppendBytes <= 0 || config.MaxAppendBytes > ProtocolMaxAppendBytes {
		return fmt.Errorf("maximum append bytes must be positive and at most %d", ProtocolMaxAppendBytes)
	}
	if config.MaxAppendEnvelopeBytes <= 0 {
		return fmt.Errorf("maximum append envelope bytes must be positive")
	}
	if config.MaxConcurrentAppendRequests <= 0 {
		return fmt.Errorf("maximum concurrent append requests must be positive")
	}
	if config.OrphanTTL <= 0 || config.CleanupInterval <= 0 {
		return fmt.Errorf("orphan TTL and cleanup interval must be positive")
	}
	return nil
}
