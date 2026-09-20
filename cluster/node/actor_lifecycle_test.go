package node

import (
	"sync/atomic"
	"testing"
	"time"
)

func spawnLifecycleActor(t *testing.T, opts ...ActorOption) (*Node, *Actor) {
	t.Helper()

	n := NewNode(WithID("actor-lifecycle-node"), WithName("game"))
	base := []ActorOption{
		WithActorKind("player"),
		WithActorID(t.Name()),
		WithActorNonWait(),
		WithActorNonDispatch(),
	}
	actor, err := n.Proxy().Spawn(
		func(*Actor, ...any) Processor { return &BaseProcessor{} },
		append(base, opts...)...,
	)
	if err != nil {
		t.Fatalf("Spawn: %v", err)
	}
	t.Cleanup(func() { actor.Destroy() })
	return n, actor
}

func TestActorIdleTimeoutReleasesActorAndRunsHook(t *testing.T) {
	released := make(chan struct{}, 1)
	n, actor := spawnLifecycleActor(t,
		WithActorIdleTimeout(25*time.Millisecond),
		WithActorReleaseHook(func(*Actor) {
			released <- struct{}{}
		}),
	)

	actor.Idle()

	select {
	case <-released:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("idle actor was not released")
	}

	if _, ok := n.Proxy().Actor(actor.Kind(), actor.ID()); ok {
		t.Fatal("idle release left actor registered in scheduler")
	}
}

func TestActorActiveCancelsPendingIdleRelease(t *testing.T) {
	var releases atomic.Int32
	n, actor := spawnLifecycleActor(t,
		WithActorIdleTimeout(30*time.Millisecond),
		WithActorReleaseHook(func(*Actor) {
			releases.Add(1)
		}),
	)

	actor.Idle()
	time.Sleep(10 * time.Millisecond)
	actor.Active()
	time.Sleep(60 * time.Millisecond)

	if got := releases.Load(); got != 0 {
		t.Fatalf("release hook calls=%d, want 0", got)
	}
	if got, ok := n.Proxy().Actor(actor.Kind(), actor.ID()); !ok || got != actor {
		t.Fatal("Active did not keep actor registered")
	}
}

func TestActorIdleReleaseGuardCanDeferRelease(t *testing.T) {
	var allow atomic.Bool
	var releases atomic.Int32
	n, actor := spawnLifecycleActor(t,
		WithActorIdleTimeout(25*time.Millisecond),
		WithActorReleaseGuard(func(*Actor) bool {
			return allow.Load()
		}),
		WithActorReleaseHook(func(*Actor) {
			releases.Add(1)
		}),
	)

	actor.Idle()
	time.Sleep(60 * time.Millisecond)

	if got := releases.Load(); got != 0 {
		t.Fatalf("guarded release hook calls=%d, want 0", got)
	}
	if got, ok := n.Proxy().Actor(actor.Kind(), actor.ID()); !ok || got != actor {
		t.Fatal("release guard did not keep actor registered")
	}

	allow.Store(true)
	actor.Idle()

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if releases.Load() == 1 {
			if _, ok := n.Proxy().Actor(actor.Kind(), actor.ID()); ok {
				t.Fatal("released actor still registered")
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("actor was not released after guard allowed release")
}

func TestActorIdleGenerationIgnoresStaleTimer(t *testing.T) {
	released := make(chan struct{}, 1)
	n, actor := spawnLifecycleActor(t,
		WithActorIdleTimeout(80*time.Millisecond),
		WithActorReleaseHook(func(*Actor) {
			released <- struct{}{}
		}),
	)

	actor.Idle()
	time.Sleep(50 * time.Millisecond)
	actor.Active()
	actor.Idle()

	// The first idle generation would have expired by now, while the second
	// generation must still be alive.
	time.Sleep(45 * time.Millisecond)
	if _, ok := n.Proxy().Actor(actor.Kind(), actor.ID()); !ok {
		t.Fatal("stale idle timer released the active generation")
	}

	select {
	case <-released:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("current idle generation did not release actor")
	}
}

func TestActorTouchAliasesActive(t *testing.T) {
	n, actor := spawnLifecycleActor(t, WithActorIdleTimeout(25*time.Millisecond))

	actor.Idle()
	time.Sleep(10 * time.Millisecond)
	actor.Touch()
	time.Sleep(50 * time.Millisecond)

	if got, ok := n.Proxy().Actor(actor.Kind(), actor.ID()); !ok || got != actor {
		t.Fatal("Touch did not cancel pending idle release")
	}
}

func TestActorReleaseHookRunsOnceOnExplicitDestroy(t *testing.T) {
	var releases atomic.Int32
	_, actor := spawnLifecycleActor(t,
		WithActorReleaseHook(func(*Actor) {
			releases.Add(1)
		}),
	)

	if !actor.Destroy() {
		t.Fatal("first Destroy returned false")
	}
	if actor.Destroy() {
		t.Fatal("second Destroy returned true")
	}
	if got := releases.Load(); got != 1 {
		t.Fatalf("release hook calls=%d, want 1", got)
	}
}

func TestActorTryInvokeReportsWhetherCallbackWasQueued(t *testing.T) {
	_, actor := spawnLifecycleActor(t)

	called := make(chan struct{}, 1)
	if !actor.TryInvoke(func() { called <- struct{}{} }) {
		t.Fatal("TryInvoke returned false for started actor")
	}
	select {
	case <-called:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("TryInvoke callback was not executed")
	}

	if !actor.Destroy() {
		t.Fatal("Destroy returned false")
	}
	if actor.TryInvoke(func() { t.Fatal("destroyed actor executed TryInvoke callback") }) {
		t.Fatal("TryInvoke returned true for destroyed actor")
	}
}

func TestActorStaleIdleTimerCannotReleaseReplacementActor(t *testing.T) {
	n := NewNode(WithID("actor-stale-idle-node"), WithName("game"))
	actorID := t.Name()
	opts := []ActorOption{
		WithActorKind("player"),
		WithActorID(actorID),
		WithActorNonWait(),
		WithActorNonDispatch(),
	}

	oldActor, err := n.Proxy().Spawn(
		func(*Actor, ...any) Processor { return &BaseProcessor{} },
		append(opts, WithActorIdleTimeout(25*time.Millisecond))...,
	)
	if err != nil {
		t.Fatalf("spawn old actor: %v", err)
	}
	t.Cleanup(func() { oldActor.destroy() })

	oldActor.Idle()

	removed, ok := n.scheduler.remove(oldActor.Kind(), oldActor.ID())
	if !ok || removed != oldActor {
		t.Fatal("failed to simulate old actor removal before destroy finalization")
	}

	replacement, err := n.Proxy().Spawn(
		func(*Actor, ...any) Processor { return &BaseProcessor{} },
		opts...,
	)
	if err != nil {
		t.Fatalf("spawn replacement actor: %v", err)
	}
	t.Cleanup(func() { replacement.Destroy() })

	time.Sleep(60 * time.Millisecond)

	if got, ok := n.Proxy().Actor(replacement.Kind(), replacement.ID()); !ok || got != replacement {
		t.Fatal("stale idle timer released the replacement actor")
	}
}
