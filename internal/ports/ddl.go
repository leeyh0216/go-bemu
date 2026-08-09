package ports

import (
	"context"

	"github.com/leeyh0216/go-bemu/internal/domain"
)

// DDLParser recognizes one GoogleSQL statement and reduces supported catalog
// DDL to a semantic command. matched is false for non-DDL statements.
type DDLParser interface {
	ParseDDL(context.Context, QueryRequest) (command domain.DDLCommand, matched bool, err error)
}
