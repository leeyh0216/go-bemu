package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type Resource interface {
	Start(context.Context) error
	Close(context.Context) error
}

// FuncResource adapts explicit lifecycle functions to Resource. It keeps
// startup ownership in the bootstrap graph without requiring each small
// runtime component to declare a one-off type.
type FuncResource struct {
	StartFunc func(context.Context) error
	CloseFunc func(context.Context) error
}

func (r FuncResource) Start(ctx context.Context) error {
	if r.StartFunc == nil {
		return nil
	}
	return r.StartFunc(ctx)
}

func (r FuncResource) Close(ctx context.Context) error {
	if r.CloseFunc == nil {
		return nil
	}
	return r.CloseFunc(ctx)
}

type Registry struct {
	mu              sync.Mutex
	resources       []Resource
	started, closed bool
}

// Runtime is the explicit Build -> Start -> Close owner for a composed
// resource graph. Build registers resources without starting goroutines;
// Start and Close provide the only lifecycle transitions.
type Runtime struct{ registry Registry }

func Build(configure func(*Runtime) error) (*Runtime, error) {
	runtime := &Runtime{}
	if configure == nil {
		return runtime, nil
	}
	if err := configure(runtime); err != nil {
		return nil, errors.Join(err, runtime.Close(context.Background()))
	}
	return runtime, nil
}

func (r *Runtime) Add(resource Resource) error     { return r.registry.Add(resource) }
func (r *Runtime) Start(ctx context.Context) error { return r.registry.Start(ctx) }
func (r *Runtime) Close(ctx context.Context) error { return r.registry.Close(ctx) }

func (r *Registry) Add(resource Resource) error {
	if resource == nil {
		return fmt.Errorf("bootstrap resource is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started || r.closed {
		return fmt.Errorf("bootstrap lifecycle is already running")
	}
	r.resources = append(r.resources, resource)
	return nil
}
func (r *Registry) Start(ctx context.Context) error {
	r.mu.Lock()
	if r.started || r.closed {
		r.mu.Unlock()
		return fmt.Errorf("bootstrap lifecycle is already running")
	}
	r.started = true
	resources := append([]Resource(nil), r.resources...)
	r.mu.Unlock()
	for _, resource := range resources {
		if err := resource.Start(ctx); err != nil {
			// Registration transfers cleanup ownership to the registry. In
			// particular, a listener may have been opened while the graph was
			// built even though its Start method has not run yet. Close every
			// registered resource so a partial start cannot leak it.
			r.mu.Lock()
			r.closed = true
			r.mu.Unlock()
			return errors.Join(err, closeReverse(ctx, resources))
		}
	}
	return nil
}
func (r *Registry) Close(ctx context.Context) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	resources := append([]Resource(nil), r.resources...)
	r.mu.Unlock()
	return closeReverse(ctx, resources)
}
func closeReverse(ctx context.Context, resources []Resource) error {
	var joined error
	for i := len(resources) - 1; i >= 0; i-- {
		joined = errors.Join(joined, resources[i].Close(ctx))
	}
	return joined
}
