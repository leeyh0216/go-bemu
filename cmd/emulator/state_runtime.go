package main

import (
	"context"
	"fmt"

	sqlitestate "github.com/leeyh0216/go-bemu/internal/adapters/sqlite"
	loadports "github.com/leeyh0216/go-bemu/internal/loadjob/ports"
	"github.com/leeyh0216/go-bemu/internal/ports"
	readports "github.com/leeyh0216/go-bemu/internal/storageread/ports"
	writeports "github.com/leeyh0216/go-bemu/internal/storagewrite/ports"
)

// stateRuntime is composition-owned. Application services receive only the
// catalog port and cannot access SQLite lifecycle or transaction internals.
type stateRuntime struct {
	catalog      ports.CatalogRepository
	queryJobs    ports.JobRepository
	loadJobs     loadports.JobRepository
	readSessions readports.SessionStateRepository
	writeState   writeports.StateRepository
	close        func() error
}

func composeStateRuntime(ctx context.Context, dsn string) (*stateRuntime, error) {
	repositories, err := sqlitestate.Open(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("open BQEMU state: %w", err)
	}
	return &stateRuntime{
		catalog: repositories.Catalog(), queryJobs: repositories.QueryJobs(),
		loadJobs: repositories.LoadJobs(), readSessions: repositories.ReadSessions(),
		writeState: repositories.WriteState(),
		close:      repositories.Close,
	}, nil
}

func (runtime *stateRuntime) Close() error {
	if runtime == nil || runtime.close == nil {
		return nil
	}
	closeState := runtime.close
	runtime.close = nil
	return closeState()
}
