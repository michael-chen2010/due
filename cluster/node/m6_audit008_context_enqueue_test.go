package node

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestM6Audit008SaturatedActorQueueCancellationDoesNotBlockDestroy(t *testing.T) {
	_, actor := spawnLifecycleActor(t)
	entered := make(chan struct{})
	unblock := make(chan struct{})
	if !actor.TryInvoke(func() { close(entered); <-unblock }) {
		t.Fatal("failed to enqueue blocking operation")
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(unblock)
		t.Fatal("Actor did not enter blocking operation")
	}
	// Saturate the exact Actor channel capacity without executing callbacks.
	for i := 0; i < cap(actor.fnChan); i++ {
		if !actor.TryInvoke(func() {}) {
			close(unblock)
			t.Fatalf("queue fill rejected at %d", i)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Millisecond)
	defer cancel()
	var executed atomic.Bool
	started := time.Now()
	queued := actor.TryInvokeContext(ctx, func() { executed.Store(true) })
	elapsed := time.Since(started)
	if queued || !errors.Is(ctx.Err(), context.DeadlineExceeded) ||
		elapsed < 20*time.Millisecond || elapsed > 300*time.Millisecond {
		close(unblock)
		t.Fatalf("expired queued call not promptly canceled queued=%v elapsed=%v err=%v", queued, elapsed, ctx.Err())
	}
	destroyed := make(chan bool, 1)
	go func() { destroyed <- actor.Destroy() }()
	// Destroy must not wait for a timed-out pending enqueue holding rw.RLock.
	// The executing callback still owns the Actor until we release it.
	close(unblock)
	select {
	case result := <-destroyed:
		if !result {
			t.Fatal("destroy did not complete")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expired queued request held Actor lock and stranded Destroy")
	}
	if executed.Load() {
		t.Fatal("timed-out business request executed after deadline")
	}
}

func TestM6Audit008InvokeContextAlreadyCanceledNeverQueues(t *testing.T) {
	_, actor := spawnLifecycleActor(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var executed atomic.Bool
	if actor.TryInvokeContext(ctx, func() { executed.Store(true) }) {
		t.Fatal("canceled request was accepted")
	}
	if actor.TryInvokeContext(nil, func() { executed.Store(true) }) {
		t.Fatal("nil context accepted")
	}
	time.Sleep(25 * time.Millisecond)
	if executed.Load() {
		t.Fatal("stale callback executed")
	}
	actor.Destroy()
	if actor.TryInvokeContext(context.Background(), func() { executed.Store(true) }) {
		t.Fatal("destroyed actor accepted callback")
	}
}
