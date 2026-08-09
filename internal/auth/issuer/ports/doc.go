// Package ports owns replaceable boundaries used by the local token issuer.
//
// Raw credentials and signed assertions may cross verifier ports only for the
// duration of one bounded operation. Stores receive digests, timestamps, and
// counts only; they never receive an access token or source credential.
package ports
