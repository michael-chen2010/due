package gate

import (
	"testing"

	"github.com/dobyte/due/v2/cluster"
)

func TestRequestMetadataUsesConfiguredTraceContextGenerator(t *testing.T) {
	const (
		correlationID = "corr-trace-gate-123"
		traceParent   = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
		traceState    = "vendor=value"
	)

	var generatorInput cluster.RequestMetadata
	g := NewGate(
		WithCorrelationIDGenerator(func() string { return correlationID }),
		WithTraceContextGenerator(func(metadata cluster.RequestMetadata) (string, string) {
			generatorInput = metadata
			return traceParent, traceState
		}),
	)
	defer g.cancel()

	got := g.newRequestMetadata()
	if generatorInput.CorrelationID != correlationID {
		t.Fatalf("generator correlation_id=%q, want %q", generatorInput.CorrelationID, correlationID)
	}
	if got.CorrelationID != correlationID {
		t.Fatalf("correlation_id=%q, want %q", got.CorrelationID, correlationID)
	}
	if got.TraceParent != traceParent || got.TraceState != traceState {
		t.Fatalf("trace context=(%q,%q), want (%q,%q)", got.TraceParent, got.TraceState, traceParent, traceState)
	}
}
