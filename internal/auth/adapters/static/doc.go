// Package static implements a bounded, reloadable local bearer-token verifier.
//
// The adapter treats the YAML file as credential material: snapshots retain
// only SHA-256 token and principal digests, errors omit paths and decoder text,
// and logs contain only shapes, counts, fingerprints, and state transitions.
// A bad reload replaces the active snapshot with deny-all state; a later valid
// reload recovers without restarting the process.
//
// Security and compatibility sources:
//   - Bearer token grammar: https://www.rfc-editor.org/rfc/rfc6750.html#section-2.1
//   - Google authentication overview: https://cloud.google.com/docs/authentication
//   - Sensitive-data logging guidance: https://cloud.google.com/logging/docs/audit/best-practices
//   - YAML 1.2 data model: https://yaml.org/spec/1.2.2/
package static
