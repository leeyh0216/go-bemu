package main

import "testing"

func TestGeneratedArtifactEqualUsesJSONSemantics(t *testing.T) {
	if !generatedArtifactEqual("contract/config.schema.json", []byte("{\n  \"enabled\": true\n}\n"), []byte("{\"enabled\":true}\n")) {
		t.Fatal("equivalent JSON was rejected")
	}
	if generatedArtifactEqual("contract/config.schema.json", []byte(`{"enabled":false}`), []byte(`{"enabled":true}`)) {
		t.Fatal("different JSON was accepted")
	}
}

func TestGeneratedArtifactEqualKeepsMarkdownByteExact(t *testing.T) {
	if generatedArtifactEqual("docs/en/config-reference.md", []byte("# A\n"), []byte("# A\r\n")) {
		t.Fatal("Markdown formatting difference was accepted")
	}
}
