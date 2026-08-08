package duckdb

import (
	"context"

	readports "github.com/leeyh0216/go-bemu/internal/storageread/ports"
	writeports "github.com/leeyh0216/go-bemu/internal/storagewrite/ports"
)

var (
	_ readports.SnapshotMaterializerFactory = (*Warehouse)(nil)
	_ writeports.CoordinatorFactory         = (*Warehouse)(nil)
)

func (w *Warehouse) NewSnapshotMaterializer(
	resolver readports.TableSchemaResolver,
	config readports.SnapshotMaterializerConfig,
) (readports.SnapshotMaterializer, error) {
	return NewReadSnapshotMaterializer(w, resolver, config)
}

func (w *Warehouse) NewCoordinator(
	ctx context.Context,
	resolver writeports.TableSchemaResolver,
	config writeports.CoordinatorConfig,
) (writeports.CoordinatorRuntime, error) {
	return NewStorageWriteCoordinator(ctx, w, resolver, config)
}
