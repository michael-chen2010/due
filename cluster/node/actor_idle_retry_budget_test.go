package node

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestIdleGuardRetryDoesNotRepeatLongInitialGrace(t *testing.T) {
	var calls atomic.Int32
	first := make(chan time.Time, 1)
	released := make(chan time.Time, 1)
	_, actor := spawnLifecycleActor(t,
		WithActorIdleTimeout(1500*time.Millisecond),
		WithActorReleaseGuard(func(*Actor) bool {
			if calls.Add(1) == 1 {
				first <- time.Now()
				return false
			}
			return true
		}),
		WithActorReleaseHook(func(*Actor) { released <- time.Now() }),
	)
	started := time.Now()
	actor.Idle()
	var denied time.Time
	select {
	case denied = <-first:
	case <-time.After(3 * time.Second):
		t.Fatal("initial Idle never checked")
	}
	if denied.Sub(started) < 1400*time.Millisecond {
		t.Fatal("initial grace was shortened")
	}
	select {
	case completed := <-released:
		if completed.Sub(denied) > 1300*time.Millisecond {
			t.Fatal("retry repeated the initial grace")
		}
	case <-time.After(1300 * time.Millisecond):
		t.Fatal("recovered guard waited for the long initial grace again")
	}
}
