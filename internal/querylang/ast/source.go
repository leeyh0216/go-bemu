package ast

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// Span identifies a half-open byte range in the submitted GoogleSQL source.
// It never retains the source text.
type Span struct {
	start int
	end   int
}

func NewSpan(start, end int) (Span, error) {
	if start < 0 || end < start {
		return Span{}, fmt.Errorf("invalid source span")
	}
	return Span{start: start, end: end}, nil
}

func (span Span) Start() int { return span.start }
func (span Span) End() int   { return span.end }

// Source identifies a parsed statement without retaining its SQL or literal
// lexemes. Digest is the lowercase SHA-256 of the complete submitted source.
type Source struct {
	digest string
	span   Span
}

func NewSource(digest string, span Span) (Source, error) {
	if !digestPattern.MatchString(digest) {
		return Source{}, fmt.Errorf("invalid source digest")
	}
	if span.end < span.start {
		return Source{}, fmt.Errorf("invalid source span")
	}
	return Source{digest: digest, span: span}, nil
}

func (source Source) Digest() string { return source.digest }
func (source Source) Span() Span     { return source.span }

// NodeKey is a stable, comparable identity for analyzer bindings. Ordinal
// disambiguates distinct parse nodes that share a kind and source span.
type NodeKey struct {
	sourceDigest string
	span         Span
	kind         string
	ordinal      int
}

func NewNodeKey(sourceDigest string, span Span, kind string, ordinal int) (NodeKey, error) {
	if !digestPattern.MatchString(sourceDigest) || strings.TrimSpace(kind) == "" || ordinal < 0 {
		return NodeKey{}, fmt.Errorf("invalid node key")
	}
	return NodeKey{sourceDigest: sourceDigest, span: span, kind: kind, ordinal: ordinal}, nil
}

func (key NodeKey) SourceDigest() string { return key.sourceDigest }
func (key NodeKey) Span() Span           { return key.span }
func (key NodeKey) Kind() string         { return key.kind }
func (key NodeKey) Ordinal() int         { return key.ordinal }

// Fingerprint is safe for diagnostics and never includes source or literal
// text. The semantic binding map uses NodeKey itself as its collision-free key.
func (key NodeKey) Fingerprint() string {
	document := key.sourceDigest + "\x00" + strconv.Itoa(key.span.start) + "\x00" +
		strconv.Itoa(key.span.end) + "\x00" + key.kind + "\x00" + strconv.Itoa(key.ordinal)
	digest := sha256.Sum256([]byte(document))
	return hex.EncodeToString(digest[:])
}

func validNodeKey(key NodeKey) bool {
	return digestPattern.MatchString(key.sourceDigest) && key.span.end >= key.span.start &&
		strings.TrimSpace(key.kind) != "" && key.ordinal >= 0
}
