package objectstore

import (
	"context"
	"fmt"
	"io"
	"net/url"

	"github.com/leeyh0216/go-bemu/internal/loadjob/domain"
	loadports "github.com/leeyh0216/go-bemu/internal/loadjob/ports"
)

type Router struct {
	file loadports.ObjectStore
	gcs  loadports.ObjectStore
}

func NewRouter(file, gcs loadports.ObjectStore) (*Router, error) {
	if file == nil && gcs == nil {
		return nil, fmt.Errorf("%w: at least one object-store adapter is required", domain.ErrInvalid)
	}
	return &Router{file: file, gcs: gcs}, nil
}

// NewGCSOnlyRouter is the secure public-runtime default. file:// sources are
// enabled only when a caller deliberately supplies FileSystem to NewRouter.
func NewGCSOnlyRouter(gcs loadports.ObjectStore) (*Router, error) {
	if gcs == nil {
		return nil, fmt.Errorf("%w: GCS object-store adapter is required", domain.ErrInvalid)
	}
	return &Router{gcs: gcs}, nil
}

func (r *Router) List(ctx context.Context, rawURI string) ([]loadports.ObjectInfo, error) {
	adapter, err := r.adapter(rawURI)
	if err != nil {
		return nil, err
	}
	return adapter.List(ctx, rawURI)
}

func (r *Router) Get(ctx context.Context, rawURI string) (loadports.ObjectInfo, error) {
	adapter, err := r.adapter(rawURI)
	if err != nil {
		return loadports.ObjectInfo{}, err
	}
	return adapter.Get(ctx, rawURI)
}

func (r *Router) Open(ctx context.Context, object loadports.ObjectInfo) (io.ReadCloser, error) {
	adapter, err := r.adapter(object.URI)
	if err != nil {
		return nil, err
	}
	return adapter.Open(ctx, object)
}

func (r *Router) adapter(rawURI string) (loadports.ObjectStore, error) {
	u, err := url.Parse(rawURI)
	if err != nil {
		return nil, fmt.Errorf("%w: malformed object URI", domain.ErrInvalid)
	}
	switch u.Scheme {
	case "file":
		if r.file == nil {
			return nil, fmt.Errorf("%w: file:// load sources are disabled", domain.ErrUnsupported)
		}
		return r.file, nil
	case "gs":
		if r.gcs == nil {
			return nil, fmt.Errorf("%w: gs:// load sources are disabled", domain.ErrUnsupported)
		}
		return r.gcs, nil
	default:
		return nil, fmt.Errorf("%w: object URI scheme %q is not implemented", domain.ErrUnsupported, u.Scheme)
	}
}

var _ loadports.ObjectStore = (*Router)(nil)
