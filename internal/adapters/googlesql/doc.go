// Package googlesql owns BQEMU's pinned official GoogleSQL parser and analyzer
// boundary. It returns only immutable internal syntax and semantic contracts;
// foreign handles and submitted SQL remain inside the adapter call lifetime.
//
// Parser provenance: github.com/goccy/go-googlesql v0.4.0, GoogleSQL revision
// 36dd14aa0657ea299725504bc0f938732f58f380.
package googlesql
