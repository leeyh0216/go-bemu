package config

import (
	"strings"
	"testing"
)

// The v1alpha1 key and environment mapping stay accepted so upgrading does not
// break an existing container. The observability package owns the invariant
// that this legacy value cannot enable payload logging.
func TestDeprecatedUnsafePayloadSettingRemainsParseCompatible(t *testing.T) {
	result, err := load(nil, lookup(map[string]string{
		"BQEMU_LOG_UNSAFE_PAYLOADS": "true",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !result.Config.Logging.UnsafePayloads {
		t.Fatal("deprecated setting was not retained in the effective compatibility model")
	}
	if !strings.Contains(string(result.EffectiveYAML), "unsafePayloads: true") {
		t.Fatalf("effective configuration omitted compatibility key: %s", result.EffectiveYAML)
	}
}
