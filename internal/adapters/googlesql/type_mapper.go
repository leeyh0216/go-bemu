package googlesql

import (
	"strings"

	gsql "github.com/goccy/go-googlesql"
	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
)

func (mapper *statementMapper) mapType(statementKind queryast.StatementKind, node gsql.ASTTypeNode) (queryast.Type, error) {
	if gsqlNodeIsNil(node) {
		return nil, parserFailure()
	}
	switch typ := node.(type) {
	case *gsql.ASTSimpleType:
		return mapper.mapSimpleType(statementKind, typ)
	case *gsql.ASTArrayType:
		return mapper.mapArrayType(statementKind, typ)
	case *gsql.ASTStructType:
		return mapper.mapStructType(statementKind, typ)
	default:
		return nil, unsupportedNode(statementKind, "type", node)
	}
}

func (mapper *statementMapper) mapSimpleType(statementKind queryast.StatementKind, node *gsql.ASTSimpleType) (queryast.Type, error) {
	if collate, err := node.Collate(); err != nil {
		return nil, parserFailure()
	} else if collate != nil {
		return nil, unsupportedNode(statementKind, "type-collation", collate)
	}
	nameNode, err := node.TypeName()
	if err != nil || nameNode == nil {
		return nil, parserFailure()
	}
	parts, err := nameNode.ToIdentifierVector()
	if err != nil || len(parts) != 1 {
		return nil, unsupportedNode(statementKind, "qualified-type-name", nameNode)
	}
	kind, ok := scalarTypeKind(parts[0])
	if !ok {
		return nil, unsupportedNode(statementKind, "scalar-type", nameNode)
	}
	parametersNode, err := node.TypeParameters()
	if err != nil {
		return nil, parserFailure()
	}
	parameters, err := integerTypeParameters(parametersNode)
	if err != nil {
		return nil, err
	}
	if len(parameters) > 2 {
		return nil, unsupportedNode(statementKind, "type-parameters", parametersNode)
	}
	if len(parameters) > 0 && kind != queryast.TypeNumeric && kind != queryast.TypeBigNumeric {
		return nil, unsupportedNode(statementKind, "type-parameters", parametersNode)
	}
	var precision, scale *int64
	if len(parameters) >= 1 {
		precision = &parameters[0]
	}
	if len(parameters) == 2 {
		scale = &parameters[1]
	}
	key, err := mapper.key(node, "scalar-type")
	if err != nil {
		return nil, err
	}
	typ, err := queryast.NewScalarType(key, kind, precision, scale)
	if err != nil {
		return nil, unsupportedNode(statementKind, "scalar-type-parameters", node)
	}
	return typ, nil
}

func (mapper *statementMapper) mapArrayType(statementKind queryast.StatementKind, node *gsql.ASTArrayType) (queryast.Type, error) {
	if collate, err := node.Collate(); err != nil {
		return nil, parserFailure()
	} else if collate != nil {
		return nil, unsupportedNode(statementKind, "array-type-collation", collate)
	}
	if parameters, err := node.TypeParameters(); err != nil {
		return nil, parserFailure()
	} else if parameters != nil {
		return nil, unsupportedNode(statementKind, "array-type-parameters", parameters)
	}
	elementNode, err := node.ElementType()
	if err != nil || elementNode == nil {
		return nil, parserFailure()
	}
	element, err := mapper.mapType(statementKind, elementNode)
	if err != nil {
		return nil, err
	}
	key, err := mapper.key(node, "array-type")
	if err != nil {
		return nil, err
	}
	return queryast.NewArrayType(key, element)
}

func (mapper *statementMapper) mapStructType(statementKind queryast.StatementKind, node *gsql.ASTStructType) (queryast.Type, error) {
	if collate, err := node.Collate(); err != nil {
		return nil, parserFailure()
	} else if collate != nil {
		return nil, unsupportedNode(statementKind, "struct-type-collation", collate)
	}
	if parameters, err := node.TypeParameters(); err != nil {
		return nil, parserFailure()
	} else if parameters != nil {
		return nil, unsupportedNode(statementKind, "struct-type-parameters", parameters)
	}
	children, err := astChildren(node)
	if err != nil {
		return nil, err
	}
	fields := make([]queryast.StructTypeField, 0, len(children))
	for _, child := range children {
		fieldNode, ok := child.(*gsql.ASTStructField)
		if !ok {
			return nil, unsupportedNode(statementKind, "struct-type-child", child)
		}
		nameNode, inspectErr := fieldNode.Name()
		if inspectErr != nil {
			return nil, parserFailure()
		}
		var name *queryast.Identifier
		if nameNode != nil {
			identifier, mapErr := mapIdentifier(nameNode)
			if mapErr != nil {
				return nil, mapErr
			}
			name = &identifier
		}
		typeNode, inspectErr := fieldNode.Type()
		if inspectErr != nil || typeNode == nil {
			return nil, parserFailure()
		}
		fieldType, mapErr := mapper.mapType(statementKind, typeNode)
		if mapErr != nil {
			return nil, mapErr
		}
		field, mapErr := queryast.NewStructTypeField(name, fieldType)
		if mapErr != nil {
			return nil, parserFailure()
		}
		fields = append(fields, field)
	}
	key, err := mapper.key(node, "struct-type")
	if err != nil {
		return nil, err
	}
	return queryast.NewStructType(key, fields)
}

func scalarTypeKind(value string) (queryast.TypeKind, bool) {
	switch strings.ToUpper(value) {
	case "BOOL", "BOOLEAN":
		return queryast.TypeBool, true
	case "INT64", "INTEGER":
		return queryast.TypeInt64, true
	case "FLOAT64", "FLOAT":
		return queryast.TypeFloat64, true
	case "NUMERIC", "DECIMAL":
		return queryast.TypeNumeric, true
	case "BIGNUMERIC", "BIGDECIMAL":
		return queryast.TypeBigNumeric, true
	case "STRING":
		return queryast.TypeString, true
	case "BYTES":
		return queryast.TypeBytes, true
	case "DATE":
		return queryast.TypeDate, true
	case "DATETIME":
		return queryast.TypeDatetime, true
	case "TIME":
		return queryast.TypeTime, true
	case "TIMESTAMP":
		return queryast.TypeTimestamp, true
	case "JSON":
		return queryast.TypeJSON, true
	case "GEOGRAPHY":
		return queryast.TypeGeography, true
	default:
		return "", false
	}
}
