package node

import (
	"context"
	"testing"
)

type setContextKey struct{}

func TestRequestSetContextReplacesCurrentContext(t *testing.T) {
	base := context.WithValue(context.Background(), setContextKey{}, "base")
	replacement := context.WithValue(base, "trace", "active")
	r := &request{ctx: base}

	r.SetContext(replacement)

	if got := r.Context().Value(setContextKey{}); got != "base" {
		t.Fatalf("preserved base value=%v", got)
	}
	if got := r.Context().Value("trace"); got != "active" {
		t.Fatalf("replacement trace value=%v", got)
	}
}

func TestEventSetContextReplacesCurrentContext(t *testing.T) {
	base := context.WithValue(context.Background(), setContextKey{}, "base")
	replacement := context.WithValue(base, "trace", "active")
	e := &event{ctx: base}

	e.SetContext(replacement)

	if got := e.Context().Value(setContextKey{}); got != "base" {
		t.Fatalf("preserved base value=%v", got)
	}
	if got := e.Context().Value("trace"); got != "active" {
		t.Fatalf("replacement trace value=%v", got)
	}
}
