package domain

import (
	"fmt"
	"net/url"
	"strings"
)

// GCSObjectURI is the validated, immutable identity of one public load source.
// It is shared by admission and the outbound object-store adapter so invalid
// schemes cannot create a job or cause an HTTP request.
type GCSObjectURI struct {
	bucket string
	object string
}

func ParseGCSObjectURI(rawURI string) (GCSObjectURI, error) {
	uri, err := url.Parse(rawURI)
	if err != nil || uri.Scheme != "gs" || uri.Host == "" || uri.User != nil ||
		uri.Port() != "" || uri.Opaque != "" || uri.RawQuery != "" || uri.Fragment != "" {
		return GCSObjectURI{}, fmt.Errorf("%w: sourceUris must contain only gs:// object URIs", ErrInvalid)
	}
	object := strings.TrimPrefix(uri.Path, "/")
	if object == "" {
		return GCSObjectURI{}, fmt.Errorf("%w: GCS object name is required", ErrInvalid)
	}
	return GCSObjectURI{bucket: uri.Host, object: object}, nil
}

func (u GCSObjectURI) Bucket() string     { return u.bucket }
func (u GCSObjectURI) ObjectName() string { return u.object }
