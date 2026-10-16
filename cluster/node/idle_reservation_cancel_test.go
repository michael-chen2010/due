package node

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dobyte/due/v2/cluster"
)

func TestIdleSuccessfulGuardCanceledByNewIdleOrDestroy(t *testing.T) {
	for _, action := range []string{"new_idle", "destroy"} {
		t.Run(action, func(t *testing.T) {
			canceled := make(chan struct{}, 2)
			var attempts atomic.Int32
			_, actor := spawnLifecycleActor(t,
				WithActorIdleTimeout(10*time.Millisecond),
				WithActorReleaseGuard(func(a *Actor) bool {
					if attempts.Add(1) > 1 {
						a.Active()
						return false
					}
					if action == "new_idle" {
						a.Idle()
					} else {
						a.Destroy()
					}
					return true
				}),
				WithActorReleaseGuardCanceled(func(*Actor) { canceled <- struct{}{} }),
			)
			actor.Idle()
			select {
			case <-canceled:
			case <-time.After(time.Second):
				t.Fatal("reservation cancellation absent")
			}
			actor.Active()
			select {
			case <-canceled:
				t.Fatal("reservation canceled twice")
			default:
			}
		})
	}
}

type delayedDestroyProcessor struct {
	BaseProcessor
	entered chan struct{}
	release chan struct{}
}

func (p *delayedDestroyProcessor) Destroy() { close(p.entered); <-p.release }

func TestDelayedOldDestroyPreservesReplacementBinding(t *testing.T) {
	n := NewNode(WithID("replacement-binding"), WithName("game"))
	p := &delayedDestroyProcessor{entered: make(chan struct{}), release: make(chan struct{})}
	old, err := n.Proxy().Spawn(func(*Actor, ...any) Processor { return p }, WithActorKind("player"), WithActorID("same"), WithActorNonWait())
	if err != nil {
		t.Fatal(err)
	}
	var unblock sync.Once
	t.Cleanup(func() { unblock.Do(func() { close(p.release) }); old.Destroy() })
	if err := n.Proxy().BindActor(91101, "player", "same"); err != nil {
		t.Fatal(err)
	}
	finished := make(chan struct{})
	go func() { old.Destroy(); close(finished) }()
	select {
	case <-p.entered:
	case <-time.After(time.Second):
		t.Fatal("destroy did not enter processor")
	}
	replacement, err := n.Proxy().Spawn(func(*Actor, ...any) Processor { return &BaseProcessor{} }, WithActorKind("player"), WithActorID("same"), WithActorNonWait())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { replacement.Destroy() })
	if err := n.Proxy().BindActor(91101, "player", "same"); err != nil {
		t.Fatal(err)
	}
	unblock.Do(func() { close(p.release) })
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("destroy did not finish")
	}
	if current, ok := replacement.scheduler.loadActor(91101, "player"); !ok || current != replacement {
		t.Fatal("delayed old destruction erased replacement binding")
	}
}

func TestIdleSuccessfulGuardCanceledByActiveReleasesReservation(t *testing.T) {
	canceled := make(chan struct{}, 1)
	var destroyed atomic.Int32
	n, actor := spawnLifecycleActor(t,
		WithActorIdleTimeout(10*time.Millisecond),
		WithActorReleaseGuard(func(a *Actor) bool { a.Active(); return true }),
		WithActorReleaseGuardCanceled(func(*Actor) { canceled <- struct{}{} }),
		WithActorReleaseHook(func(*Actor) { destroyed.Add(1) }),
	)
	actor.Idle()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("successful guard reservation was not canceled")
	}
	if current, ok := n.Proxy().Actor(actor.Kind(), actor.ID()); !ok || current != actor || destroyed.Load() != 0 {
		t.Fatal("reactivated actor was destroyed")
	}
}

func TestIdleDeniedGuardDoesNotCancelUnownedReservation(t *testing.T) {
	var called atomic.Int32
	var canceled atomic.Int32
	_, actor := spawnLifecycleActor(t,
		WithActorIdleTimeout(10*time.Millisecond),
		WithActorReleaseGuard(func(a *Actor) bool { a.Active(); called.Add(1); return false }),
		WithActorReleaseGuardCanceled(func(*Actor) { canceled.Add(1) }),
	)
	// Invoke the timer callback synchronously, so its return is a real
	// completion barrier including the code after the unlocked guard.
	actor.lifecycleMu.Lock()
	generation := actor.idleGeneration
	actor.lifecycleMu.Unlock()
	actor.releaseIdleGeneration(generation)
	if called.Load() != 1 || canceled.Load() != 0 {
		t.Fatal("denied guard canceled a reservation it never acquired")
	}
}

func TestIdleCommitRejectsStaleGenerationAndOldInstance(t *testing.T) {
	n, actor := spawnLifecycleActor(t)
	actor.lifecycleMu.Lock()
	generation := actor.idleGeneration
	actor.lifecycleMu.Unlock()
	actor.Active()
	if actor.scheduler.killIdleActor(actor, generation) {
		t.Fatal("stale idle generation committed destroy")
	}
	if current, ok := n.Proxy().Actor(actor.Kind(), actor.ID()); !ok || current != actor {
		t.Fatal("stale generation removed live actor")
	}
	if !actor.Destroy() {
		t.Fatal("explicit destroy failed")
	}
	replacement, err := n.Proxy().Spawn(func(*Actor, ...any) Processor { return &BaseProcessor{} }, WithActorKind(actor.Kind()), WithActorID(actor.ID()), WithActorNonWait())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { replacement.Destroy() })
	if actor.scheduler.killIdleActor(actor, generation) {
		t.Fatal("old instance idle callback committed")
	}
	if current, ok := n.Proxy().Actor(replacement.Kind(), replacement.ID()); !ok || current != replacement {
		t.Fatal("old instance removed replacement")
	}
}

func TestCanceledIdleReservationKeepsWaitUntilActualDestroy(t *testing.T) {
	n := NewNode(WithID("idle-wait"), WithName("game"))
	// Exercise the Work-state accounting without starting network listeners.
	n.state.Store(int32(cluster.Work))
	var canceled atomic.Int32
	actor, err := n.Proxy().Spawn(func(*Actor, ...any) Processor { return &BaseProcessor{} },
		WithActorKind("player"), WithActorID("wait"),
		WithActorReleaseGuard(func(a *Actor) bool { a.Active(); return true }),
		WithActorReleaseGuardCanceled(func(*Actor) { canceled.Add(1) }),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { actor.Destroy() })
	actor.releaseIdleGeneration(0)
	if canceled.Load() != 1 || n.pendingWork.Load() != 1 {
		t.Fatal("canceled idle changed actual actor wait accounting")
	}
	if !actor.Destroy() || actor.Destroy() || n.pendingWork.Load() != 0 {
		t.Fatal("actual destroy did not decrement exactly once")
	}
	finished := make(chan struct{})
	go func() { n.wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("wait group remained blocked")
	}
}
