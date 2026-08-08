package domain

import (
	"fmt"
	"reflect"
	"strings"
)

// DDLKind identifies the bounded GoogleSQL catalog mutations supported by the
// emulator. SQL text is intentionally absent from DDLCommand.
type DDLKind string

const (
	DDLCreateTable     DDLKind = "CREATE_TABLE"
	DDLDropTable       DDLKind = "DROP_TABLE"
	DDLTruncateTable   DDLKind = "TRUNCATE_TABLE"
	DDLAddColumn       DDLKind = "ADD_COLUMN"
	DDLDropColumn      DDLKind = "DROP_COLUMN"
	DDLRenameColumn    DDLKind = "RENAME_COLUMN"
	DDLAlterColumnType DDLKind = "ALTER_COLUMN_TYPE"
)

// DDLCommandDescriptor is mutable constructor input. DDLCommand owns recursive
// copies and exposes only detached values to application and engine ports.
type DDLCommandDescriptor struct {
	Kind    DDLKind
	Table   TableReference
	Schema  []Field
	Field   Field
	Name    string
	NewName string
}

// DDLCommand is the semantic output of the GoogleSQL adapter. Concrete storage
// adapters never receive the client SQL that produced it.
type DDLCommand struct {
	kind    DDLKind
	table   TableReference
	schema  []Field
	field   Field
	name    string
	newName string
}

func NewDDLCommand(descriptor DDLCommandDescriptor) (DDLCommand, error) {
	command := DDLCommand{
		kind: descriptor.Kind, table: descriptor.Table,
		schema: CloneFields(descriptor.Schema), field: cloneDDLField(descriptor.Field),
		name: descriptor.Name, newName: descriptor.NewName,
	}
	if err := command.validate(); err != nil {
		return DDLCommand{}, err
	}
	return command, nil
}

func (command DDLCommand) Kind() DDLKind         { return command.kind }
func (command DDLCommand) Table() TableReference { return command.table }
func (command DDLCommand) Schema() []Field       { return CloneFields(command.schema) }
func (command DDLCommand) Field() Field          { return cloneDDLField(command.field) }
func (command DDLCommand) Name() string          { return command.name }
func (command DDLCommand) NewName() string       { return command.newName }
func (command DDLCommand) Validate() error       { return command.validate() }

func (command DDLCommand) validate() error {
	if err := validateDDLTableReference(command.table); err != nil {
		return err
	}
	zeroField := reflect.DeepEqual(command.field, Field{})
	switch command.kind {
	case DDLCreateTable:
		if !zeroField || command.name != "" || command.newName != "" {
			return invalidDDLCommand("CREATE TABLE contains an ALTER payload")
		}
		if err := (Table{
			ProjectID: command.table.ProjectID, DatasetID: command.table.DatasetID,
			ID: command.table.TableID, Type: "TABLE", Schema: command.schema,
		}).Validate(); err != nil {
			return err
		}
	case DDLDropTable, DDLTruncateTable:
		if len(command.schema) != 0 || !zeroField || command.name != "" || command.newName != "" {
			return invalidDDLCommand("table command contains a column payload")
		}
	case DDLAddColumn:
		if len(command.schema) != 0 || zeroField || command.name != "" || command.newName != "" {
			return invalidDDLCommand("ADD COLUMN payload is invalid")
		}
		if err := command.field.Validate(); err != nil {
			return err
		}
	case DDLDropColumn:
		if len(command.schema) != 0 || !zeroField || command.newName != "" || !validDDLFieldName(command.name) {
			return invalidDDLCommand("DROP COLUMN payload is invalid")
		}
	case DDLRenameColumn:
		if len(command.schema) != 0 || !zeroField || !validDDLFieldName(command.name) ||
			!validDDLFieldName(command.newName) || strings.EqualFold(command.name, command.newName) {
			return invalidDDLCommand("RENAME COLUMN payload is invalid")
		}
	case DDLAlterColumnType:
		if len(command.schema) != 0 || zeroField || !validDDLFieldName(command.name) ||
			!strings.EqualFold(command.name, command.field.Name) {
			return invalidDDLCommand("ALTER COLUMN SET DATA TYPE payload is invalid")
		}
		if err := command.field.Validate(); err != nil {
			return err
		}
	default:
		return invalidDDLCommand("DDL kind is not supported")
	}
	return nil
}

func validateDDLTableReference(reference TableReference) error {
	return (Table{
		ProjectID: reference.ProjectID, DatasetID: reference.DatasetID,
		ID: reference.TableID, Schema: []Field{{Name: "placeholder", Type: "STRING"}},
	}).Validate()
}

func validDDLFieldName(name string) bool {
	return (Field{Name: name, Type: "STRING"}).Validate() == nil
}

func cloneDDLField(field Field) Field {
	if reflect.DeepEqual(field, Field{}) {
		return Field{}
	}
	return CloneFields([]Field{field})[0]
}

func invalidDDLCommand(detail string) error {
	return fmt.Errorf("%w: %s", ErrInvalid, detail)
}
