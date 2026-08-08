package ast

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
)

type semanticWriter interface {
	writeSemantic(*fingerprintBuilder)
}

type fingerprintBuilder struct {
	bytes []byte
}

func (builder *fingerprintBuilder) token(value string) {
	builder.bytes = strconv.AppendInt(builder.bytes, int64(len(value)), 10)
	builder.bytes = append(builder.bytes, ':')
	builder.bytes = append(builder.bytes, value...)
	builder.bytes = append(builder.bytes, ';')
}

func (builder *fingerprintBuilder) boolean(value bool) {
	if value {
		builder.token("true")
		return
	}
	builder.token("false")
}

func semanticFingerprint(value semanticWriter) string {
	builder := &fingerprintBuilder{}
	value.writeSemantic(builder)
	digest := sha256.Sum256(builder.bytes)
	return hex.EncodeToString(digest[:])
}

func writeIdentifier(builder *fingerprintBuilder, identifier Identifier) {
	builder.token(identifier.value)
}

func writePath(builder *fingerprintBuilder, path IdentifierPath) {
	builder.token(strconv.Itoa(len(path.parts)))
	for _, part := range path.parts {
		writeIdentifier(builder, part)
	}
}
