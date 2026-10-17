package due

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
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

type trackedShutdownComponent struct {
	component.Base
	closeRelease chan struct{}
	destroyCalls atomic.Int32
}

func (c *trackedShutdownComponent) Close()   { <-c.closeRelease }
func (c *trackedShutdownComponent) Destroy() { c.destroyCalls.Add(1) }

func TestContainerShutdownCloseTimeoutNeverRunsConcurrentDestroy(t *testing.T) {
	const key = "etc.shutdownMaxWaitTime"
	original := etc.Get(key).String()
	t.Cleanup(func() { _ = etc.Set(key, original) })
	if err := etc.Set(key, 25*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	c := NewContainer()
	blocked := &trackedShutdownComponent{closeRelease: make(chan struct{})}
	c.Add(blocked)
	started := time.Now()
	err := c.finishShutdown()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected close deadline: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("shutdown elapsed %v", elapsed)
	}
	if blocked.destroyCalls.Load() != 0 {
		t.Fatal("Destroy ran while Close was still in flight")
	}
	if !errors.Is(c.ShutdownError(), context.DeadlineExceeded) {
		t.Fatalf("shutdown status lost failed Close: %v", c.ShutdownError())
	}
	close(blocked.closeRelease)
	if again := c.finishShutdown(); !errors.Is(again, context.DeadlineExceeded) {
		t.Fatalf("retry must keep first uncertain shutdown: %v", again)
	}
	if blocked.destroyCalls.Load() != 0 {
		t.Fatal("late Close or repeated shutdown must not launch Destroy")
	}
}

func TestContainerShutdownSuccessfulCloseRunsDestroy(t *testing.T) {
	c := NewContainer()
	tracked := &trackedShutdownComponent{closeRelease: make(chan struct{})}
	close(tracked.closeRelease)
	c.Add(tracked)
	if err := c.finishShutdown(); err != nil {
		t.Fatal(err)
	}
	if tracked.destroyCalls.Load() != 1 {
		t.Fatal("normal shutdown skipped Destroy")
	}
	if c.ShutdownError() != nil {
		t.Fatalf("successful shutdown reported failure: %v", c.ShutdownError())
	}
}

func TestContainerShutdownDestroyFailureIsSticky(t *testing.T) {
	const key = "etc.shutdownMaxWaitTime"
	original := etc.Get(key).String()
	t.Cleanup(func() { _ = etc.Set(key, original) })
	if err := etc.Set(key, 25*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	c := NewContainer()
	blocked := &blockingDestroyComponent{release: make(chan struct{})}
	c.Add(blocked)
	if err := c.finishShutdown(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("destroy deadline expected: %v", err)
	}
	close(blocked.release)
	if err := c.finishShutdown(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("destroy error must remain sticky: %v", err)
	}
	if !errors.Is(c.ShutdownError(), context.DeadlineExceeded) {
		t.Fatal("destroy failure was lost")
	}
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
