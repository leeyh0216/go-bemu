package main

import (
	"context"
	"fmt"

	sqlitestate "github.com/leeyh0216/go-bemu/internal/adapters/sqlite"
	"github.com/leeyh0216/go-bemu/internal/ports"
)

// stateRuntime is composition-owned. Application services receive only the
// catalog port and cannot access SQLite lifecycle or transaction internals.
type stateRuntime struct {
	catalog ports.CatalogRepository
	close   func() error
}

func composeStateRuntime(ctx context.Context, dsn string) (*stateRuntime, error) {
	repositories, err := sqlitestate.Open(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("open BQEMU state: %w", err)
	}
	return &stateRuntime{catalog: repositories.Catalog(), close: repositories.Close}, nil
}

func (runtime *stateRuntime) Close() error {
	if runtime == nil || runtime.close == nil {
		return nil
	}
	closeState := runtime.close
	runtime.close = nil
	return closeState()
}
