package ast

import (
	"fmt"
	"strings"
)

// Identifier is the decoded semantic value of one GoogleSQL identifier. Its
// original quoting and source spelling are intentionally not retained.
type Identifier struct {
	value string
}

func NewIdentifier(value string) (Identifier, error) {
	if value == "" || strings.IndexByte(value, 0) >= 0 {
		return Identifier{}, fmt.Errorf("invalid identifier")
	}
	return Identifier{value: value}, nil
}

func (identifier Identifier) Value() string { return identifier.value }

// IdentifierPath is an unresolved, segment-oriented name from the parse AST.
// It must not be confused with a catalog-resolved domain.TableReference.
type IdentifierPath struct {
	parts []Identifier
}

func NewIdentifierPath(parts []Identifier) (IdentifierPath, error) {
	if len(parts) == 0 {
		return IdentifierPath{}, fmt.Errorf("identifier path is empty")
	}
	cloned := append([]Identifier(nil), parts...)
	for _, part := range cloned {
		if _, err := NewIdentifier(part.value); err != nil {
			return IdentifierPath{}, err
		}
	}
	return IdentifierPath{parts: cloned}, nil
}

func (path IdentifierPath) Parts() []Identifier {
	return append([]Identifier(nil), path.parts...)
}

func (path IdentifierPath) Segments() []string {
	segments := make([]string, len(path.parts))
	for index, part := range path.parts {
		segments[index] = part.value
	}
	return segments
}

func (path IdentifierPath) Len() int { return len(path.parts) }
