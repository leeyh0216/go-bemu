package googlesql

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"runtime"

	gsql "github.com/goccy/go-googlesql"
	"github.com/leeyh0216/go-bemu/internal/domain"
	"github.com/leeyh0216/go-bemu/internal/ports"
	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
)

const queryASTCapability = "query.google-sql-ast-v1"

// UnsupportedNodeError identifies the unsupported syntax location.
type UnsupportedNodeError struct {
	StatementKind queryast.StatementKind
	NodeKind      string
	Span          queryast.Span
}

func (err *UnsupportedNodeError) Error() string {
	return fmt.Sprintf(
		"unsupported GoogleSQL syntax node; statement_kind=%s node_kind=%s byte_start=%d byte_end=%d capability=%s",
		err.StatementKind, err.NodeKind, err.Span.Start(), err.Span.End(), queryASTCapability,
	)
}

func (*UnsupportedNodeError) Unwrap() error { return domain.ErrUnsupported }

type parsedDocument struct {
	statements []gsql.ASTStatementNode
	source     queryast.Source
	owner      *gsql.ParserOutput
}

// Parse is the single GoogleSQL syntax entrypoint. It uses the official script
// parser for both one statement and ordered multi-statement input, so statement
// classification never depends on a keyword scanner or backend parser.
func (*Parser) Parse(ctx context.Context, request ports.QueryRequest) (queryast.Statement, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	document, err := parseExternal(request.SQL)
	if err != nil {
		return nil, err
	}
	defer func() { runtime.KeepAlive(document.owner) }()
	mapper := statementMapper{sourceDigest: document.source.Digest()}
	statements := make([]queryast.Statement, 0, len(document.statements))
	for _, external := range document.statements {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		statement, err := mapper.mapStatement(external)
		if err != nil {
			return nil, err
		}
		statements = append(statements, statement)
	}
	if len(statements) == 1 {
		return statements[0], nil
	}
	return queryast.NewScriptStatement(document.source, statements)
}

// parseExternal remains package-private so a later gateway can pass the same
// official AST handles to both the syntax mapper and GoogleSQL analyzer without
// exposing those handles through an application or engine port.
func parseExternal(sql string) (parsedDocument, error) {
	if err := initialize(); err != nil {
		return parsedDocument{}, err
	}
	options, err := parserOptions()
	if err != nil {
		return parsedDocument{}, parserFailure(err)
	}
	output, err := gsql.ParseScript(sql, options, nil)
	if err != nil || output == nil {
		return parsedDocument{}, invalidInput("invalid GoogleSQL statement syntax", sql, err)
	}
	script, err := output.Script()
	if err != nil || script == nil {
		return parsedDocument{}, parserFailure(err)
	}
	list, err := script.StatementListNode()
	if err != nil || list == nil {
		return parsedDocument{}, parserFailure(err)
	}
	count, err := list.NumChildren()
	if err != nil || count <= 0 {
		return parsedDocument{}, invalid("GoogleSQL statement is empty", err)
	}
	statements := make([]gsql.ASTStatementNode, 0, count)
	for index := int32(0); index < count; index++ {
		statement, err := list.StatementList(index)
		if err != nil || statement == nil {
			return parsedDocument{}, parserFailure(err)
		}
		statements = append(statements, statement)
	}
	source, err := sourceForInput(sql)
	if err != nil {
		return parsedDocument{}, err
	}
	return parsedDocument{statements: statements, source: source, owner: output}, nil
}

func sourceForInput(input string) (queryast.Source, error) {
	digest := sha256.Sum256([]byte(input))
	span, err := queryast.NewSpan(0, len(input))
	if err != nil {
		return queryast.Source{}, parserFailure()
	}
	source, err := queryast.NewSource(hex.EncodeToString(digest[:]), span)
	if err != nil {
		return queryast.Source{}, parserFailure()
	}
	return source, nil
}

type statementMapper struct {
	sourceDigest string
	nextOrdinal  int
}

func (mapper *statementMapper) source(node gsql.ASTNode) (queryast.Source, error) {
	span, err := sourceSpan(node)
	if err != nil {
		return queryast.Source{}, err
	}
	source, err := queryast.NewSource(mapper.sourceDigest, span)
	if err != nil {
		return queryast.Source{}, parserFailure()
	}
	return source, nil
}

func (mapper *statementMapper) key(node gsql.ASTNode, kind string) (queryast.NodeKey, error) {
	span, err := sourceSpan(node)
	if err != nil {
		return queryast.NodeKey{}, err
	}
	key, err := queryast.NewNodeKey(mapper.sourceDigest, span, kind, mapper.nextOrdinal)
	if err != nil {
		return queryast.NodeKey{}, parserFailure()
	}
	mapper.nextOrdinal++
	return key, nil
}

func sourceSpan(node gsql.ASTNode) (queryast.Span, error) {
	location, err := node.GetParseLocationRange()
	if err != nil || location == nil {
		return queryast.Span{}, parserFailure()
	}
	start, err := location.Start()
	if err != nil || start == nil {
		return queryast.Span{}, parserFailure()
	}
	end, err := location.End()
	if err != nil || end == nil {
		return queryast.Span{}, parserFailure()
	}
	startOffset, err := start.GetByteOffset()
	if err != nil {
		return queryast.Span{}, parserFailure()
	}
	endOffset, err := end.GetByteOffset()
	if err != nil {
		return queryast.Span{}, parserFailure()
	}
	span, err := queryast.NewSpan(int(startOffset), int(endOffset))
	if err != nil {
		return queryast.Span{}, parserFailure()
	}
	return span, nil
}

func unsupportedNode(statementKind queryast.StatementKind, nodeKind string, node gsql.ASTNode) error {
	span, err := sourceSpan(node)
	if err != nil {
		return err
	}
	return &UnsupportedNodeError{StatementKind: statementKind, NodeKind: nodeKind, Span: span}
}
