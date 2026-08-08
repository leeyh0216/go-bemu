package googlesql

import (
	"strings"

	gsql "github.com/goccy/go-googlesql"
	"github.com/leeyh0216/go-bemu/internal/domain"
	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
)

var ddlValidationReference = domain.TableReference{ProjectID: "validation", DatasetID: "validation", TableID: "validation"}

func (mapper *statementMapper) mapCreateTableStatement(node *gsql.ASTCreateTableStatement) (queryast.Statement, error) {
	if enabled, err := node.IsIfNotExists(); err != nil {
		return nil, parserFailure()
	} else if enabled {
		return nil, unsupportedNode(queryast.StatementCreateTable, "create-if-not-exists", node)
	}
	if enabled, err := node.IsOrReplace(); err != nil {
		return nil, parserFailure()
	} else if enabled {
		return nil, unsupportedNode(queryast.StatementCreateTable, "create-or-replace", node)
	}
	if unsupportedScope, err := hasUnsupportedCreateScope(node); err != nil {
		return nil, err
	} else if unsupportedScope {
		return nil, unsupportedNode(queryast.StatementCreateTable, "create-scope", node)
	}
	if err := rejectCreateTableClauses(node); err != nil {
		return nil, unsupportedNode(queryast.StatementCreateTable, "create-table-clause", node)
	}
	nameNode, err := node.Name()
	if err != nil || nameNode == nil {
		return nil, parserFailure()
	}
	target, err := mapper.mapTargetRelation(nameNode, nil)
	if err != nil {
		return nil, err
	}
	elements, err := node.TableElementList()
	if err != nil || elements == nil {
		return nil, parserFailure()
	}
	constraints, err := elements.HasConstraints()
	if err != nil {
		return nil, parserFailure()
	}
	if constraints {
		return nil, unsupportedNode(queryast.StatementCreateTable, "table-constraint", elements)
	}
	children, err := astChildren(elements)
	if err != nil {
		return nil, err
	}
	columns := make([]queryast.ColumnDefinition, 0, len(children))
	for _, child := range children {
		definition, ok := child.(*gsql.ASTColumnDefinition)
		if !ok {
			return nil, unsupportedNode(queryast.StatementCreateTable, "table-element", child)
		}
		column, mapErr := mapper.mapDDLColumnDefinition(queryast.StatementCreateTable, definition)
		if mapErr != nil {
			return nil, mapErr
		}
		columns = append(columns, column)
	}
	source, err := mapper.source(node)
	if err != nil {
		return nil, err
	}
	statement, err := queryast.NewCreateTableStatement(source, target, columns)
	if err != nil {
		return nil, parserFailure()
	}
	return statement, nil
}

func (mapper *statementMapper) mapDropStatement(node *gsql.ASTDropStatement) (queryast.Statement, error) {
	kind, err := node.SchemaObjectKind()
	if err != nil {
		return nil, parserFailure()
	}
	if kind != gsql.SchemaObjectKindKTable {
		return nil, unsupportedNode(queryast.StatementDropTable, "drop-object-kind", node)
	}
	if exists, err := node.IsIfExists(); err != nil {
		return nil, parserFailure()
	} else if exists {
		return nil, unsupportedNode(queryast.StatementDropTable, "drop-if-exists", node)
	}
	mode, err := node.DropMode()
	if err != nil {
		return nil, parserFailure()
	}
	if mode != gsql.ASTDropStatementEnums_DropModeDropModeUnspecified {
		return nil, unsupportedNode(queryast.StatementDropTable, "drop-mode", node)
	}
	nameNode, err := node.Name()
	if err != nil || nameNode == nil {
		return nil, parserFailure()
	}
	target, err := mapper.mapTargetRelation(nameNode, nil)
	if err != nil {
		return nil, err
	}
	source, err := mapper.source(node)
	if err != nil {
		return nil, err
	}
	return queryast.NewDropTableStatement(source, target)
}

func (mapper *statementMapper) mapTruncateStatement(node *gsql.ASTTruncateStatement) (queryast.Statement, error) {
	where, err := node.Where()
	if err != nil {
		return nil, parserFailure()
	}
	if where != nil {
		return nil, unsupportedNode(queryast.StatementTruncateTable, "truncate-where", where)
	}
	pathNode, err := node.TargetPath()
	if err != nil || pathNode == nil {
		return nil, parserFailure()
	}
	target, err := mapper.mapTargetRelation(pathNode, nil)
	if err != nil {
		return nil, err
	}
	source, err := mapper.source(node)
	if err != nil {
		return nil, err
	}
	return queryast.NewTruncateTableStatement(source, target)
}

func (mapper *statementMapper) mapAlterTableStatement(node *gsql.ASTAlterTableStatement) (queryast.Statement, error) {
	if exists, err := node.IsIfExists(); err != nil {
		return nil, parserFailure()
	} else if exists {
		return nil, unsupportedNode(queryast.StatementAlterTable, "alter-if-exists", node)
	}
	pathNode, err := node.Path()
	if err != nil || pathNode == nil {
		return nil, parserFailure()
	}
	target, err := mapper.mapTargetRelation(pathNode, nil)
	if err != nil {
		return nil, err
	}
	actions, err := node.ActionList()
	if err != nil || actions == nil {
		return nil, parserFailure()
	}
	children, err := astChildren(actions)
	if err != nil {
		return nil, err
	}
	if len(children) != 1 {
		return nil, unsupportedNode(queryast.StatementAlterTable, "alter-action-count", actions)
	}
	action, err := mapper.mapAlterAction(children[0])
	if err != nil {
		return nil, err
	}
	source, err := mapper.source(node)
	if err != nil {
		return nil, err
	}
	return queryast.NewAlterTableStatement(source, target, action)
}

func (mapper *statementMapper) mapAlterAction(node gsql.ASTNode) (queryast.AlterAction, error) {
	switch action := node.(type) {
	case *gsql.ASTAddColumnAction:
		command, err := parseAddColumn(action, ddlValidationReference)
		if err != nil {
			return queryast.AlterAction{}, err
		}
		column, err := mapper.columnDefinitionFromField(queryast.StatementAlterTable, action, command.Field())
		if err != nil {
			return queryast.AlterAction{}, err
		}
		return queryast.NewAlterAddColumnAction(column)
	case *gsql.ASTDropColumnAction:
		command, err := parseDropColumn(action, ddlValidationReference)
		if err != nil {
			return queryast.AlterAction{}, err
		}
		name, err := queryast.NewIdentifier(command.Name())
		if err != nil {
			return queryast.AlterAction{}, parserFailure()
		}
		return queryast.NewAlterDropColumnAction(name)
	case *gsql.ASTRenameColumnAction:
		command, err := parseRenameColumn(action, ddlValidationReference)
		if err != nil {
			return queryast.AlterAction{}, err
		}
		name, err := queryast.NewIdentifier(command.Name())
		if err != nil {
			return queryast.AlterAction{}, parserFailure()
		}
		newName, err := queryast.NewIdentifier(command.NewName())
		if err != nil {
			return queryast.AlterAction{}, parserFailure()
		}
		return queryast.NewAlterRenameColumnAction(name, newName)
	case *gsql.ASTAlterColumnTypeAction:
		command, err := parseAlterColumnType(action, ddlValidationReference)
		if err != nil {
			return queryast.AlterAction{}, err
		}
		name, err := queryast.NewIdentifier(command.Name())
		if err != nil {
			return queryast.AlterAction{}, parserFailure()
		}
		typ, err := mapper.typeFromDomainField(queryast.StatementAlterTable, action, command.Field())
		if err != nil {
			return queryast.AlterAction{}, err
		}
		return queryast.NewAlterColumnTypeAction(name, typ)
	default:
		return queryast.AlterAction{}, unsupportedNode(queryast.StatementAlterTable, "alter-action", node)
	}
}

func (mapper *statementMapper) mapDDLColumnDefinition(statementKind queryast.StatementKind, node *gsql.ASTColumnDefinition) (queryast.ColumnDefinition, error) {
	field, err := parseColumnDefinition(node)
	if err != nil {
		return queryast.ColumnDefinition{}, err
	}
	return mapper.columnDefinitionFromField(statementKind, node, field)
}

func (mapper *statementMapper) columnDefinitionFromField(statementKind queryast.StatementKind, node gsql.ASTNode, field domain.Field) (queryast.ColumnDefinition, error) {
	name, err := queryast.NewIdentifier(field.Name)
	if err != nil {
		return queryast.ColumnDefinition{}, parserFailure()
	}
	typ, err := mapper.typeFromDomainField(statementKind, node, field)
	if err != nil {
		return queryast.ColumnDefinition{}, err
	}
	column, err := queryast.NewColumnDefinition(name, typ, field.Mode == "REQUIRED")
	if err != nil {
		return queryast.ColumnDefinition{}, parserFailure()
	}
	return column, nil
}

func (mapper *statementMapper) typeFromDomainField(statementKind queryast.StatementKind, node gsql.ASTNode, field domain.Field) (queryast.Type, error) {
	var typ queryast.Type
	if strings.EqualFold(field.Type, "STRUCT") || strings.EqualFold(field.Type, "RECORD") {
		fields := make([]queryast.StructTypeField, 0, len(field.Fields))
		for _, nested := range field.Fields {
			name, err := queryast.NewIdentifier(nested.Name)
			if err != nil {
				return nil, parserFailure()
			}
			nestedType, err := mapper.typeFromDomainField(statementKind, node, nested)
			if err != nil {
				return nil, err
			}
			structField, err := queryast.NewStructTypeField(&name, nestedType)
			if err != nil {
				return nil, parserFailure()
			}
			fields = append(fields, structField)
		}
		key, err := mapper.key(node, "struct-type")
		if err != nil {
			return nil, err
		}
		typ, err = queryast.NewStructType(key, fields)
		if err != nil {
			return nil, parserFailure()
		}
	} else {
		kind, ok := scalarTypeKind(field.Type)
		if !ok {
			return nil, unsupportedNode(statementKind, "column-type", node)
		}
		key, err := mapper.key(node, "scalar-type")
		if err != nil {
			return nil, err
		}
		typ, err = queryast.NewScalarType(key, kind, field.Precision, field.Scale)
		if err != nil {
			return nil, unsupportedNode(statementKind, "column-type-parameters", node)
		}
	}
	if field.Mode != "REPEATED" {
		return typ, nil
	}
	key, err := mapper.key(node, "array-type")
	if err != nil {
		return nil, err
	}
	array, err := queryast.NewArrayType(key, typ)
	if err != nil {
		return nil, parserFailure()
	}
	return array, nil
}
