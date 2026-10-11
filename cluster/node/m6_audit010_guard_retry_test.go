package node

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestM6Audit010IdleGuardTransientFailureRetriesAutomatically(t *testing.T) {
	var guardCalls atomic.Int32
	released := make(chan struct{}, 1)
	n, actor := spawnLifecycleActor(t,
		WithActorIdleTimeout(20*time.Millisecond),
		WithActorReleaseGuard(func(*Actor) bool {
			return guardCalls.Add(1) >= 2 // first Redis Current lookup failed
		}),
		WithActorReleaseHook(func(*Actor) { released <- struct{}{} }),
	)
	actor.Idle() // no second Idle is ever issued
	select {
	case <-released:
	case <-time.After(1000 * time.Millisecond):
		t.Fatal("failed ReleaseGuard stranded idle Actor after Redis recovered")
	}
	if guardCalls.Load() < 2 {
		t.Fatalf("guard did not retry after transient failure: attempts=%d", guardCalls.Load())
	}
	if _, alive := n.Proxy().Actor(actor.Kind(), actor.ID()); alive {
		t.Fatal("auto-retried Actor release left old Actor in scheduler")
	}
}

func TestM6Audit010ActiveCancelsRetryAfterGuardFailure(t *testing.T) {
	firstAttempt := make(chan struct{}, 1)
	var attempts atomic.Int32
	var hooks atomic.Int32
	n, actor := spawnLifecycleActor(t,
		WithActorIdleTimeout(35*time.Millisecond),
		WithActorReleaseGuard(func(*Actor) bool {
			if attempts.Add(1) == 1 {
				firstAttempt <- struct{}{}
				return false
			}
			return true
		}),
		WithActorReleaseHook(func(*Actor) { hooks.Add(1) }),
	)
	actor.Idle()
	select {
	case <-firstAttempt:
	case <-time.After(time.Second):
		t.Fatal("idle guard was not consulted")
	}
	actor.Active() // a session reconnected before retry fired
	time.Sleep(125 * time.Millisecond)
	if attempts.Load() != 1 || hooks.Load() != 0 {
		t.Fatalf("Active did not cancel stale retry attempts=%d hooks=%d", attempts.Load(), hooks.Load())
	}
	if current, ok := n.Proxy().Actor(actor.Kind(), actor.ID()); !ok || current != actor {
		t.Fatal("Active player released by stale guard retry")
	}
}

func TestM6Audit010PersistentGuardFailureBoundedAndDestroyStopsRetry(t *testing.T) {
	var attempts atomic.Int32
	n, actor := spawnLifecycleActor(t,
		WithActorIdleTimeout(20*time.Millisecond),
		WithActorReleaseGuard(func(*Actor) bool {
			attempts.Add(1)
			return false
		}),
	)
	actor.Idle()
	time.Sleep(115 * time.Millisecond)
	before := attempts.Load()
	if before < 2 || before > 8 {
		t.Fatalf("permanently failing guard either did not retry or busy-looped: attempts=%d", before)
	}
	if current, ok := n.Proxy().Actor(actor.Kind(), actor.ID()); !ok || current != actor {
		t.Fatal("guard failure destroyed Actor without authorization")
	}
	if !actor.Destroy() {
		t.Fatal("explicit destroy rejected")
	}
	time.Sleep(80 * time.Millisecond)
	after := attempts.Load()
	if after > before+1 {
		t.Fatalf("destroyed Actor continued guard retry: before=%d after=%d", before, after)
	}
}
