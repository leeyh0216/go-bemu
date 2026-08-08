package ast

import (
	"fmt"
	"strconv"
	"strings"
)

type TypeKind string

const (
	TypeBool       TypeKind = "BOOL"
	TypeInt64      TypeKind = "INT64"
	TypeFloat64    TypeKind = "FLOAT64"
	TypeNumeric    TypeKind = "NUMERIC"
	TypeBigNumeric TypeKind = "BIGNUMERIC"
	TypeString     TypeKind = "STRING"
	TypeBytes      TypeKind = "BYTES"
	TypeDate       TypeKind = "DATE"
	TypeDatetime   TypeKind = "DATETIME"
	TypeTime       TypeKind = "TIME"
	TypeTimestamp  TypeKind = "TIMESTAMP"
	TypeJSON       TypeKind = "JSON"
	TypeGeography  TypeKind = "GEOGRAPHY"
	TypeArray      TypeKind = "ARRAY"
	TypeStruct     TypeKind = "STRUCT"
)

type Type interface {
	Kind() TypeKind
	NodeKey() NodeKey
	Span() Span
	Accept(TypeVisitor) error
	typeNode()
	semanticWriter
}

type TypeVisitor interface {
	VisitScalarType(*ScalarType) error
	VisitArrayType(*ArrayType) error
	VisitStructType(*StructType) error
}

type typeBase struct {
	key NodeKey
}

func (base typeBase) NodeKey() NodeKey { return base.key }
func (base typeBase) Span() Span       { return base.key.span }

type ScalarType struct {
	typeBase
	kind      TypeKind
	precision *int64
	scale     *int64
}

func NewScalarType(key NodeKey, kind TypeKind, precision, scale *int64) (*ScalarType, error) {
	switch kind {
	case TypeBool, TypeInt64, TypeFloat64, TypeNumeric, TypeBigNumeric, TypeString,
		TypeBytes, TypeDate, TypeDatetime, TypeTime, TypeTimestamp, TypeJSON, TypeGeography:
	default:
		return nil, fmt.Errorf("unsupported scalar type kind")
	}
	if precision == nil && scale != nil {
		return nil, fmt.Errorf("type scale requires precision")
	}
	if precision != nil && kind != TypeNumeric && kind != TypeBigNumeric {
		return nil, fmt.Errorf("type parameters require a decimal type")
	}
	if precision != nil {
		effectiveScale := int64(0)
		if scale != nil {
			effectiveScale = *scale
		}
		if *precision <= 0 || effectiveScale < 0 || effectiveScale > *precision {
			return nil, fmt.Errorf("invalid decimal type parameters")
		}
	}
	return &ScalarType{typeBase: typeBase{key: key}, kind: kind, precision: cloneInt64(precision), scale: cloneInt64(scale)}, nil
}

func (*ScalarType) typeNode()                            {}
func (typ *ScalarType) Kind() TypeKind                   { return typ.kind }
func (typ *ScalarType) Precision() *int64                { return cloneInt64(typ.precision) }
func (typ *ScalarType) Scale() *int64                    { return cloneInt64(typ.scale) }
func (typ *ScalarType) Accept(visitor TypeVisitor) error { return visitor.VisitScalarType(typ) }
func (typ *ScalarType) writeSemantic(builder *fingerprintBuilder) {
	builder.token("scalar-type")
	builder.token(string(typ.kind))
	if typ.precision == nil {
		builder.token("")
		builder.token("")
		return
	}
	builder.token(strconv.FormatInt(*typ.precision, 10))
	if typ.scale == nil {
		builder.token("omitted")
	} else {
		builder.token(strconv.FormatInt(*typ.scale, 10))
	}
}

type ArrayType struct {
	typeBase
	element Type
}

func NewArrayType(key NodeKey, element Type) (*ArrayType, error) {
	if typeIsNil(element) {
		return nil, fmt.Errorf("array element type is required")
	}
	return &ArrayType{typeBase: typeBase{key: key}, element: element}, nil
}

func (*ArrayType) typeNode()                            {}
func (*ArrayType) Kind() TypeKind                       { return TypeArray }
func (typ *ArrayType) Element() Type                    { return typ.element }
func (typ *ArrayType) Accept(visitor TypeVisitor) error { return visitor.VisitArrayType(typ) }
func (typ *ArrayType) writeSemantic(builder *fingerprintBuilder) {
	builder.token("array-type")
	typ.element.writeSemantic(builder)
}

type StructTypeField struct {
	name  *Identifier
	type_ Type
}

func NewStructTypeField(name *Identifier, typ Type) (StructTypeField, error) {
	if typeIsNil(typ) {
		return StructTypeField{}, fmt.Errorf("struct field type is required")
	}
	return StructTypeField{name: cloneIdentifier(name), type_: typ}, nil
}

func (field StructTypeField) Name() *Identifier { return cloneIdentifier(field.name) }
func (field StructTypeField) Type() Type        { return field.type_ }

type StructType struct {
	typeBase
	fields []StructTypeField
}

func NewStructType(key NodeKey, fields []StructTypeField) (*StructType, error) {
	cloned := append([]StructTypeField(nil), fields...)
	for _, field := range cloned {
		if typeIsNil(field.type_) {
			return nil, fmt.Errorf("struct field type is required")
		}
	}
	return &StructType{typeBase: typeBase{key: key}, fields: cloned}, nil
}

func (*StructType) typeNode()      {}
func (*StructType) Kind() TypeKind { return TypeStruct }
func (typ *StructType) Fields() []StructTypeField {
	return append([]StructTypeField(nil), typ.fields...)
}
func (typ *StructType) Accept(visitor TypeVisitor) error { return visitor.VisitStructType(typ) }
func (typ *StructType) writeSemantic(builder *fingerprintBuilder) {
	builder.token("struct-type")
	builder.token(strconv.Itoa(len(typ.fields)))
	for _, field := range typ.fields {
		if field.name == nil {
			builder.token("")
		} else {
			writeIdentifier(builder, *field.name)
		}
		field.type_.writeSemantic(builder)
	}
}

func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneIdentifier(value *Identifier) *Identifier {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func typeIsNil(value Type) bool {
	if value == nil {
		return true
	}
	return strings.TrimSpace(string(value.Kind())) == ""
}
