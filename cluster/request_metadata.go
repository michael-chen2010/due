package cluster

import (
	"context"
	"time"
)

// RequestMetadata carries immutable per-request metadata across Due transport.
// Zero values preserve the legacy behavior for callers that do not opt in.
type RequestMetadata struct {
	Deadline      time.Time
	CorrelationID string
}

type requestMetadataContextKey struct{}

// WithRequestMetadata attaches decoded transport metadata to the local provider context.
func WithRequestMetadata(ctx context.Context, metadata RequestMetadata) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, requestMetadataContextKey{}, metadata)
}

// RequestMetadataFromContext returns metadata reconstructed by Due transport.
func RequestMetadataFromContext(ctx context.Context) (RequestMetadata, bool) {
	if ctx == nil {
		return RequestMetadata{}, false
	}
	metadata, ok := ctx.Value(requestMetadataContextKey{}).(RequestMetadata)
	return metadata, ok
}
