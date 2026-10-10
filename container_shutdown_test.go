package due

import (
	"context"
	"errors"
	"strings"
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

func (c *blockingCloseComponent) ShutdownCloseStatus() string {
	return "stage=waitgroup pending_waits=1"
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
	if !strings.Contains(err.Error(), "pending close components=") ||
		!strings.Contains(err.Error(), "blockingCloseComponent") ||
		!strings.Contains(err.Error(), "stage=waitgroup pending_waits=1") {
		t.Fatalf("close timeout error must name blocking component, got %v", err)
	}
}

func TestContainerCloseSuccessReturnsNoPendingDiagnostics(t *testing.T) {
	container := NewContainer()
	container.Add(&component.Base{})
	if err := container.doCloseComponents(); err != nil {
		t.Fatalf("completed component close returned unexpected error: %v", err)
	}
}
