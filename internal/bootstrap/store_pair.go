package bootstrap

import (
	"context"
	"fmt"

	"github.com/leeyh0216/go-bemu/internal/observability"
)

// reconcileStorePair rejects a state/engine file combination unless both sides
// carry the same durable generation. A fresh pair is initialized before any
// catalog mutation or listener can become visible.
func reconcileStorePair(ctx context.Context, state *stateRuntime, engine *engineRuntime) error {
	stateGeneration, stateFound, err := state.PairGeneration(ctx)
	if err != nil {
		return err
	}
	engineGeneration, engineFound, err := engine.PairGeneration(ctx)
	if err != nil {
		return err
	}
	if !stateFound && !engineFound {
		generation := observability.NewID()
		if err := state.SetPairGeneration(ctx, generation); err != nil {
			return err
		}
		if err := engine.SetPairGeneration(ctx, generation); err != nil {
			return err
		}
		return nil
	}
	if !stateFound || !engineFound {
		return fmt.Errorf("state and DuckDB files are not the same BQEMU generation")
	}
	if stateGeneration != engineGeneration {
		return fmt.Errorf("state and DuckDB files have different BQEMU generations")
	}
	return nil
}
