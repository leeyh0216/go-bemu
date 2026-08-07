package application

import (
	"context"
	"sync"
)

// contextMutex preserves the existing single-writer catalog boundary while
// allowing read paths with a server-owned deadline to abandon admission.
// sync.Mutex has no context-aware acquisition operation, so the one-element
// channel is initialized lazily to keep the zero value usable by CatalogService.
//
// Context cancellation contract: https://pkg.go.dev/context#Context
type contextMutex struct {
	once   sync.Once
	permit chan struct{}
}

func (mutex *contextMutex) initialize() {
	mutex.once.Do(func() {
		mutex.permit = make(chan struct{}, 1)
		mutex.permit <- struct{}{}
	})
}

func (mutex *contextMutex) Lock() {
	mutex.initialize()
	<-mutex.permit
}

func (mutex *contextMutex) LockContext(ctx context.Context) error {
	mutex.initialize()
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-mutex.permit:
		// Cancellation and admission can become ready together. Give an already
		// observable cancellation precedence and return the permit immediately.
		if err := ctx.Err(); err != nil {
			mutex.permit <- struct{}{}
			return err
		}
		return nil
	}
}

func (mutex *contextMutex) Unlock() {
	mutex.initialize()
	mutex.permit <- struct{}{}
}
