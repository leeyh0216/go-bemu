package bootstrap

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type probe struct {
	name  string
	calls *[]string
}

type failingProbe struct{ probe }

func (p failingProbe) Start(context.Context) error {
	*p.calls = append(*p.calls, "start:"+p.name)
	return errors.New("start failed")
}

func TestRegistryClosesEveryRegisteredResourceAfterStartFailure(t *testing.T) {
	var calls []string
	var r Registry
	_ = r.Add(probe{"state", &calls})
	_ = r.Add(failingProbe{probe{"engine", &calls}})
	_ = r.Add(probe{"listener", &calls})
	if err := r.Start(t.Context()); err == nil {
		t.Fatal("Start() error = nil")
	}
	if err := r.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"start:state", "start:engine",
		"close:listener", "close:engine", "close:state",
	}
	if !reflect.DeepEqual(calls, want) {
		t.Fatal(calls)
	}
}

func (p probe) Start(context.Context) error { *p.calls = append(*p.calls, "start:"+p.name); return nil }
func (p probe) Close(context.Context) error { *p.calls = append(*p.calls, "close:"+p.name); return nil }
func TestRegistryClosesInReverseOrder(t *testing.T) {
	var calls []string
	var r Registry
	_ = r.Add(probe{"state", &calls})
	_ = r.Add(probe{"listener", &calls})
	if err := r.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	want := []string{"start:state", "start:listener", "close:listener", "close:state"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatal(calls)
	}
}

func TestFuncResourceAllowsOneSidedLifecycle(t *testing.T) {
	var calls []string
	resource := FuncResource{CloseFunc: func(context.Context) error {
		calls = append(calls, "close")
		return nil
	}}
	if err := resource.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := resource.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(calls, []string{"close"}) {
		t.Fatal(calls)
	}
}

func TestBuildClosesRegisteredResourcesWhenCompositionFails(t *testing.T) {
	var calls []string
	_, err := Build(func(runtime *Runtime) error {
		if err := runtime.Add(probe{"state", &calls}); err != nil {
			return err
		}
		return errors.New("engine composition failed")
	})
	if err == nil {
		t.Fatal("Build() error = nil")
	}
	if !reflect.DeepEqual(calls, []string{"close:state"}) {
		t.Fatal(calls)
	}
}
