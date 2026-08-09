package application

import (
	"fmt"

	"github.com/leeyh0216/go-bemu/internal/domain"
	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
	"github.com/leeyh0216/go-bemu/internal/querylang/semantic"
)

// ddlCommandFromStatement lowers the already analyzed GoogleSQL AST into the
// catalog use case. It never inspects or reparses the client SQL text.
func ddlCommandFromStatement(statement semantic.Statement) (domain.DDLCommand, error) {
	visitor := &ddlCommandVisitor{statement: statement}
	if syntax := statement.Syntax(); syntax == nil {
		return domain.DDLCommand{}, fmt.Errorf("%w: analyzed DDL syntax is missing", domain.ErrPrecondition)
	} else if err := syntax.Accept(visitor); err != nil {
		return domain.DDLCommand{}, err
	}
	if err := visitor.command.Validate(); err != nil {
		return domain.DDLCommand{}, err
	}
	return visitor.command, nil
}

type ddlCommandVisitor struct {
	statement semantic.Statement
	command   domain.DDLCommand
}

func (visitor *ddlCommandVisitor) VisitCreateTable(statement *queryast.CreateTableStatement) error {
	reference, err := visitor.target(statement.Target())
	if err != nil {
		return err
	}
	columns := statement.Columns()
	schema := make([]domain.Field, len(columns))
	for index, column := range columns {
		field, err := ddlFieldFromAST(column.Name().Value(), column.Type(), column.NotNull())
		if err != nil {
			return err
		}
		schema[index] = field
	}
	visitor.command, err = domain.NewDDLCommand(domain.DDLCommandDescriptor{
		Kind: domain.DDLCreateTable, Table: reference, Schema: schema,
	})
	return err
}

func (visitor *ddlCommandVisitor) VisitDropTable(statement *queryast.DropTableStatement) error {
	return visitor.tableCommand(domain.DDLDropTable, statement.Target())
}

func (visitor *ddlCommandVisitor) VisitTruncateTable(statement *queryast.TruncateTableStatement) error {
	return visitor.tableCommand(domain.DDLTruncateTable, statement.Target())
}

func (visitor *ddlCommandVisitor) VisitAlterTable(statement *queryast.AlterTableStatement) error {
	reference, err := visitor.target(statement.Target())
	if err != nil {
		return err
	}
	descriptor := domain.DDLCommandDescriptor{Table: reference}
	action := statement.Action()
	switch action.Kind() {
	case queryast.AlterAddColumn:
		column := action.Column()
		if column == nil {
			return invalidAnalyzedDDL()
		}
		descriptor.Kind = domain.DDLAddColumn
		descriptor.Field, err = ddlFieldFromAST(column.Name().Value(), column.Type(), column.NotNull())
	case queryast.AlterDropColumn:
		descriptor.Kind = domain.DDLDropColumn
		descriptor.Name = action.Name().Value()
	case queryast.AlterRenameColumn:
		descriptor.Kind = domain.DDLRenameColumn
		descriptor.Name = action.Name().Value()
		descriptor.NewName = action.NewName().Value()
	case queryast.AlterColumnType:
		column := action.Column()
		if column == nil {
			return invalidAnalyzedDDL()
		}
		descriptor.Kind = domain.DDLAlterColumnType
		descriptor.Name = action.Name().Value()
		descriptor.Field, err = ddlFieldFromAST(descriptor.Name, column.Type(), false)
	default:
		return unsupportedDDL("analyzed ALTER TABLE action is not supported")
	}
	if err != nil {
		return err
	}
	visitor.command, err = domain.NewDDLCommand(descriptor)
	return err
}

func (visitor *ddlCommandVisitor) tableCommand(kind domain.DDLKind, target *queryast.TableRelation) error {
	reference, err := visitor.target(target)
	if err != nil {
		return err
	}
	visitor.command, err = domain.NewDDLCommand(domain.DDLCommandDescriptor{Kind: kind, Table: reference})
	return err
}

func (visitor *ddlCommandVisitor) target(target *queryast.TableRelation) (domain.TableReference, error) {
	if target == nil {
		return domain.TableReference{}, invalidAnalyzedDDL()
	}
	binding, err := visitor.statement.RequireRelationBinding(target.NodeKey())
	if err != nil {
		return domain.TableReference{}, err
	}
	reference, physical := binding.Reference()
	if !physical {
		return domain.TableReference{}, invalidAnalyzedDDL()
	}
	return reference, nil
}

func (*ddlCommandVisitor) VisitScript(*queryast.ScriptStatement) error {
	return unsupportedDDL("DDL scripts require the semantic script dispatcher")
}

func (*ddlCommandVisitor) VisitDeclare(*queryast.DeclareStatement) error {
	return unsupportedDDL("DECLARE is not a catalog DDL statement")
}

func (*ddlCommandVisitor) VisitSet(*queryast.SetStatement) error {
	return unsupportedDDL("SET is not a catalog DDL statement")
}

func (*ddlCommandVisitor) VisitSelect(*queryast.SelectStatement) error {
	return unsupportedDDL("SELECT is not a catalog DDL statement")
}

func (*ddlCommandVisitor) VisitInsert(*queryast.InsertStatement) error {
	return unsupportedDDL("INSERT is not a catalog DDL statement")
}

func (*ddlCommandVisitor) VisitUpdate(*queryast.UpdateStatement) error {
	return unsupportedDDL("UPDATE is not a catalog DDL statement")
}

func (*ddlCommandVisitor) VisitDelete(*queryast.DeleteStatement) error {
	return unsupportedDDL("DELETE is not a catalog DDL statement")
}

func (*ddlCommandVisitor) VisitMerge(*queryast.MergeStatement) error {
	return unsupportedDDL("MERGE is not a catalog DDL statement")
}

func (*ddlCommandVisitor) VisitCreateView(*queryast.CreateViewStatement) error {
	return unsupportedDDL("CREATE VIEW is not a catalog DDL statement")
}

func (*ddlCommandVisitor) VisitDropView(*queryast.DropViewStatement) error {
	return unsupportedDDL("DROP VIEW is not a catalog DDL statement")
}

func ddlFieldFromAST(name string, typ queryast.Type, notNull bool) (domain.Field, error) {
	if typ == nil {
		return domain.Field{}, invalidAnalyzedDDL()
	}
	field := domain.Field{Name: name, Mode: "NULLABLE"}
	if notNull {
		field.Mode = "REQUIRED"
	}
	switch value := typ.(type) {
	case *queryast.ScalarType:
		field.Type = string(value.Kind())
		field.Precision = value.Precision()
		field.Scale = value.Scale()
	case *queryast.StructType:
		field.Type = "STRUCT"
		members := value.Fields()
		if len(members) == 0 {
			return domain.Field{}, invalidAnalyzedDDL()
		}
		field.Fields = make([]domain.Field, len(members))
		for index, member := range members {
			memberName := member.Name()
			if memberName == nil {
				return domain.Field{}, invalidAnalyzedDDL()
			}
			nested, err := ddlFieldFromAST(memberName.Value(), member.Type(), false)
			if err != nil {
				return domain.Field{}, err
			}
			field.Fields[index] = nested
		}
	case *queryast.ArrayType:
		if notNull {
			return domain.Field{}, unsupportedDDL("ARRAY nullability annotations are not supported")
		}
		element, err := ddlFieldFromAST(name, value.Element(), false)
		if err != nil {
			return domain.Field{}, err
		}
		if element.Mode == "REPEATED" {
			return domain.Field{}, unsupportedDDL("nested ARRAY types are not supported")
		}
		field = element
		field.Mode = "REPEATED"
	default:
		return domain.Field{}, unsupportedDDL("analyzed column type is not supported")
	}
	if err := field.Validate(); err != nil {
		return domain.Field{}, err
	}
	return field, nil
}

func invalidAnalyzedDDL() error {
	return fmt.Errorf("%w: analyzed DDL statement is incomplete", domain.ErrPrecondition)
}
