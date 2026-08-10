package domain

import "testing"

func TestParseCDCChangeTypeAcceptsOnlyDocumentedValues(t *testing.T) {
	for _, value := range []string{"UPSERT", "DELETE"} {
		parsed, err := ParseCDCChangeType(value)
		if err != nil || string(parsed) != value {
			t.Fatalf("ParseCDCChangeType(%q) = %q, %v", value, parsed, err)
		}
	}
	for _, value := range []string{"", "INSERT", "upsert"} {
		if _, err := ParseCDCChangeType(value); err == nil {
			t.Fatalf("ParseCDCChangeType(%q) accepted an unsupported value", value)
		}
	}
}

func TestCDCSequenceNumberValidatesDocumentedShapeAndOrdering(t *testing.T) {
	for _, value := range []string{"0", "a", "FFFFFFFFFFFFFFFF", "fff/AbC", "1/2/3/4"} {
		sequence, err := ParseCDCSequenceNumber(value)
		if err != nil || sequence.SectionCount() < 1 || sequence.SectionCount() > 4 {
			t.Fatalf("ParseCDCSequenceNumber(%q) = %#v, %v", value, sequence, err)
		}
	}
	for _, value := range []string{"", "/1", "1/", "1//2", "1/2/3/4/5", "10000000000000000", "not-hex"} {
		if _, err := ParseCDCSequenceNumber(value); err == nil {
			t.Fatalf("ParseCDCSequenceNumber(%q) accepted an invalid shape", value)
		}
	}

}
