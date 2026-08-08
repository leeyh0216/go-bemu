// Package config loads and validates the versioned BQEMU runtime model.
//
// Configuration sources:
//   - YAML 1.2 data model: https://yaml.org/spec/1.2.2/
//   - Docker bind mounts: https://docs.docker.com/engine/storage/bind-mounts/
//   - Kubernetes ConfigMaps: https://kubernetes.io/docs/concepts/configuration/configmap/
//
// Precedence is defaults < YAML file < environment < repeated --set flags.
// Unknown YAML fields fail fast so a misspelled setting cannot silently select
// a default. File-backed material remains referenced by path; the effective
// configuration and structured diagnostics otherwise retain configured values.
package config
