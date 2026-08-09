package bootstrap

import (
	"context"
	"strings"
	"testing"
)

type pairStoreFake struct {
	generation string
	found      bool
}

func (store *pairStoreFake) PairGeneration(context.Context) (string, bool, error) {
	return store.generation, store.found, nil
}
func (store *pairStoreFake) SetPairGeneration(_ context.Context, generation string) error {
	store.generation = generation
	store.found = true
	return nil
}

func TestReconcileStorePairInitializesOrRejectsMixedGenerations(t *testing.T) {
	newRuntime := func(stateStore, engineStore *pairStoreFake) (*stateRuntime, *engineRuntime) {
		return &stateRuntime{pairGeneration: stateStore.PairGeneration, setPairGeneration: stateStore.SetPairGeneration}, &engineRuntime{pairGeneration: engineStore}
	}
	stateStore, engineStore := &pairStoreFake{}, &pairStoreFake{}
	state, engine := newRuntime(stateStore, engineStore)
	if err := reconcileStorePair(t.Context(), state, engine); err != nil {
		t.Fatal(err)
	}
	if !stateStore.found || stateStore.generation == "" || stateStore.generation != engineStore.generation {
		t.Fatalf("fresh generation=%q/%q", stateStore.generation, engineStore.generation)
	}
	stateStore, engineStore = &pairStoreFake{generation: "one", found: true}, &pairStoreFake{generation: "two", found: true}
	state, engine = newRuntime(stateStore, engineStore)
	if err := reconcileStorePair(t.Context(), state, engine); err == nil || !strings.Contains(err.Error(), "different") {
		t.Fatalf("mismatch error=%v", err)
	}
	stateStore, engineStore = &pairStoreFake{generation: "one", found: true}, &pairStoreFake{}
	state, engine = newRuntime(stateStore, engineStore)
	if err := reconcileStorePair(t.Context(), state, engine); err == nil || !strings.Contains(err.Error(), "not the same") {
		t.Fatalf("one-sided error=%v", err)
	}
}
