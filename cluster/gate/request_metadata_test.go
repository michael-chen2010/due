package gate

import (
	"testing"
	"time"
)

func TestRequestMetadataDefaultsDisabled(t *testing.T) {
	g := NewGate()
	defer g.cancel()

	metadata := g.newRequestMetadata()

	if !metadata.Deadline.IsZero() {
		t.Fatalf("default metadata deadline=%v, want zero", metadata.Deadline)
	}
	if metadata.CorrelationID != "" {
		t.Fatalf("default correlation_id=%q, want empty", metadata.CorrelationID)
	}
}

func TestRequestMetadataUsesConfiguredDeadlineAndCorrelationGenerator(t *testing.T) {
	const correlationID = "corr-gate-123"
	timeout := 2 * time.Second
	g := NewGate(
		WithRequestTimeout(timeout),
		WithCorrelationIDGenerator(func() string { return correlationID }),
	)
	defer g.cancel()

	before := time.Now()
	metadata := g.newRequestMetadata()
	after := time.Now()

	if metadata.CorrelationID != correlationID {
		t.Fatalf("correlation_id=%q, want %q", metadata.CorrelationID, correlationID)
	}
	if metadata.Deadline.Before(before.Add(timeout)) || metadata.Deadline.After(after.Add(timeout)) {
		t.Fatalf("metadata deadline=%v, want within [%v, %v]", metadata.Deadline, before.Add(timeout), after.Add(timeout))
	}
}
