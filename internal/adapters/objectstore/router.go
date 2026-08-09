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
	gcs   loadports.ObjectStore
	media loadports.ObjectStore
}

func NewRouter(gcs loadports.ObjectStore) (*Router, error) {
	if gcs == nil {
		return nil, fmt.Errorf("%w: GCS object-store adapter is required", domain.ErrInvalid)
	}
	return &Router{gcs: gcs}, nil
}

// NewRouterWithMedia adds the private bqemu-upload:// scheme used only after a
// media upload has been committed. It does not enable client file:// sources.
func NewRouterWithMedia(gcs, media loadports.ObjectStore) (*Router, error) {
	if media == nil {
		return nil, fmt.Errorf("%w: media upload store is required", domain.ErrInvalid)
	}
	if gcs == nil {
		return &Router{media: media}, nil
	}
	router, err := NewRouter(gcs)
	if err != nil {
		return nil, err
	}
	router.media = media
	return router, nil
}

// NewGCSOnlyRouter is the public-runtime default.
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
		return nil, fmt.Errorf("%w: file:// load sources are disabled", domain.ErrUnsupported)
	case "gs":
		if r.gcs == nil {
			return nil, fmt.Errorf("%w: gs:// load sources are disabled", domain.ErrUnsupported)
		}
		return r.gcs, nil
	case mediaScheme:
		if r.media == nil {
			return nil, fmt.Errorf("%w: media upload objects are disabled", domain.ErrUnsupported)
		}
		return r.media, nil
	default:
		return nil, fmt.Errorf("%w: object URI scheme %q is not implemented", domain.ErrUnsupported, u.Scheme)
	}
}

var _ loadports.ObjectStore = (*Router)(nil)
