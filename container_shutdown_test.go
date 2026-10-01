package due

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dobyte/due/v2/component"
	"github.com/dobyte/due/v2/etc"
)

type blockingDestroyComponent struct {
	component.Base
	release chan struct{}
}

func (c *blockingDestroyComponent) Destroy() {
	<-c.release
}

func TestContainerDestroyUsesConfiguredShutdownMaxWaitTime(t *testing.T) {
	const key = "etc.shutdownMaxWaitTime"
	original := etc.Get(key).String()
	t.Cleanup(func() {
		_ = etc.Set(key, original)
	})
	if err := etc.Set(key, 25*time.Millisecond); err != nil {
		t.Fatalf("set shutdown timeout: %v", err)
	}

	comp := &blockingDestroyComponent{release: make(chan struct{})}
	container := NewContainer()
	container.Add(comp)

	start := time.Now()
	err := container.doDestroyComponents()
	elapsed := time.Since(start)
	close(comp.release)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("destroy error=%v, want context.DeadlineExceeded", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("destroy elapsed=%v, want configured shutdown deadline near 25ms", elapsed)
	}
}

type blockingCloseComponent struct {
	component.Base
	release chan struct{}
}

func (c *blockingCloseComponent) Close() {
	<-c.release
}

func TestContainerCloseReportsConfiguredShutdownDeadline(t *testing.T) {
	const key = "etc.shutdownMaxWaitTime"
	original := etc.Get(key).String()
	t.Cleanup(func() {
		_ = etc.Set(key, original)
	})
	if err := etc.Set(key, 25*time.Millisecond); err != nil {
		t.Fatalf("set shutdown timeout: %v", err)
	}

	comp := &blockingCloseComponent{release: make(chan struct{})}
	container := NewContainer()
	container.Add(comp)

	err := container.doCloseComponents()
	close(comp.release)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close error=%v, want context.DeadlineExceeded", err)
	}
}
