// Package domain defines the transport-neutral local OAuth token issuer model.
//
// The issuer reproduces only the protocol contract needed by local Google auth
// clients. It does not emulate Google IAM, OAuth consent, workforce pools, or
// Google's production token service policy.
//
// Protocol sources:
//   - OAuth refresh grants and responses: https://www.rfc-editor.org/rfc/rfc6749.html#section-6
//   - JWT bearer grants: https://www.rfc-editor.org/rfc/rfc7523.html#section-2.1
//   - OAuth token exchange: https://www.rfc-editor.org/rfc/rfc8693.html#section-2
//   - Google service-account requests: https://developers.google.com/identity/protocols/oauth2/service-account#httprest
package domain
