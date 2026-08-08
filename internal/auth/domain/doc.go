// Package domain defines BQEMU's local authentication contract.
//
// Authentication proves only that a request presented a credential accepted by
// the configured local verifier. It does not emulate Google Cloud IAM roles,
// permission inheritance, token introspection, or federation policy.
//
// Protocol and security sources:
//   - https://www.rfc-editor.org/rfc/rfc6750.html
//   - https://cloud.google.com/docs/authentication
//   - https://cloud.google.com/logging/docs/audit/best-practices
package domain
