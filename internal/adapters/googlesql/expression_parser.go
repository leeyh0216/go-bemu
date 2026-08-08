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
// execution backend and never returns the submitted expression text.
func (*Gateway) ParseExpression(ctx context.Context, input string) (queryast.Expression, error) {
	return parseExpression(ctx, input)
}

// ParseExpression remains on Parser while transport/application tests migrate
// to Gateway. Production composition uses Gateway for statements and
// expressions so both entrypoints share one official boundary.
func (*Parser) ParseExpression(ctx context.Context, input string) (queryast.Expression, error) {
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
		return nil, parserFailure()
	}
	output, err := gsql.ParseExpression(input, options)
	if err != nil || output == nil {
		return nil, invalid("invalid GoogleSQL expression syntax")
	}
	defer func() { runtime.KeepAlive(output) }()
	external, err := output.Expression()
	if err != nil || external == nil {
		return nil, parserFailure()
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
