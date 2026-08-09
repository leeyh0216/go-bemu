// Package memory provides a bounded, replaceable issued-token store.
//
// Only SHA-256 digests and protocol metadata are retained. Commit couples a
// token record to its optional replay marker under one mutex so concurrent
// uses of one JWT assertion cannot both succeed.
package memory
