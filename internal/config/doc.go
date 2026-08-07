// Package config loads and validates the versioned BQEMU runtime model.
//
// Configuration sources:
//   - YAML 1.2 data model: https://yaml.org/spec/1.2.2/
//   - Docker bind mounts: https://docs.docker.com/engine/storage/bind-mounts/
//   - Kubernetes ConfigMaps: https://kubernetes.io/docs/concepts/configuration/configmap/
//
// Precedence is defaults < YAML file < environment < repeated --set flags.
// Unknown YAML fields fail fast so a misspelled setting cannot silently select
// a default. Secret material is referenced by file path and is never embedded
// in the effective configuration or its structured diagnostics.
package config
