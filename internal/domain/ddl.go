package domain

// DDLKind identifies the bounded catalog mutations accepted from GoogleSQL.
// SQL text is intentionally absent from DDLCommand so storage adapters cannot
// accidentally execute client-provided DDL directly.
type DDLKind string

const (
	DDLCreateTable     DDLKind = "CREATE_TABLE"
	DDLDropTable       DDLKind = "DROP_TABLE"
	DDLAddColumn       DDLKind = "ADD_COLUMN"
	DDLDropColumn      DDLKind = "DROP_COLUMN"
	DDLRenameColumn    DDLKind = "RENAME_COLUMN"
	DDLAlterColumnType DDLKind = "ALTER_COLUMN_TYPE"
)

// DDLCommand is the semantic form produced by the GoogleSQL parser and
// consumed by the catalog-owned DDL executor.
type DDLCommand struct {
	Kind    DDLKind
	Table   TableReference
	Schema  []Field
	Field   Field
	Name    string
	NewName string
}
