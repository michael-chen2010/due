package gate

import (
	"errors"
	"testing"

	"github.com/dobyte/due/v2/cluster"
)

func TestRequestMetadataTraceContextLifecycleCompletesWithDeliveryResult(t *testing.T) {
	wantErr := errors.New("deliver failed")
	var completedErr error

	g := NewGate(
		WithTraceContextGenerator(func(cluster.RequestMetadata) (string, string, func(error)) {
			return "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", "vendor=value", func(err error) {
				completedErr = err
			}
		}),
	)
	defer g.cancel()

	metadata, complete := g.newRequestMetadataWithTraceLifecycle()
	if metadata.TraceParent == "" {
		t.Fatal("traceparent is blank")
	}
	if complete == nil {
		t.Fatal("trace completion callback is nil")
	}
	complete(wantErr)
	if !errors.Is(completedErr, wantErr) {
		t.Fatalf("completed error=%v, want %v", completedErr, wantErr)
	}
}

func TestTraceContextCompletionPanicIsolated(t *testing.T) {
	completeTraceContextLifecycle(func(error) {
		panic("trace callback failed")
	}, errors.New("deliver failed"))
}
