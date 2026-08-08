package googlesql

import (
	"context"
	"runtime"

	gsql "github.com/goccy/go-googlesql"
	queryast "github.com/leeyh0216/go-bemu/internal/querylang/ast"
)

const expressionContextKind queryast.StatementKind = "EXPRESSION"

// ParseExpression is the GoogleSQL predicate entrypoint used by APIs such as
// Storage Read row_restriction. It never delegates syntax recognition to an
// execution backend.
func (*Gateway) ParseExpression(ctx context.Context, input string) (queryast.Expression, error) {
	return parseExpression(ctx, input)
}

func parseExpression(ctx context.Context, input string) (queryast.Expression, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := initialize(); err != nil {
		return nil, err
	}
	options, err := parserOptions()
	if err != nil {
		return nil, parserFailure(err)
	}
	output, err := gsql.ParseExpression(input, options)
	if err != nil || output == nil {
		return nil, invalidInput("invalid GoogleSQL expression syntax", input, err)
	}
	defer func() { runtime.KeepAlive(output) }()
	external, err := output.Expression()
	if err != nil || external == nil {
		return nil, parserFailure(err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	source, err := sourceForInput(input)
	if err != nil {
		return nil, err
	}
	mapper := statementMapper{sourceDigest: source.Digest()}
	return mapper.mapExpression(expressionContextKind, external)
}
