package node

import (
	"testing"
	"time"
)

func TestSchedulerRemoveActorSerializesRelationMutation(t *testing.T) {
	s := newScheduler(nil)
	actor := &Actor{
		opts:      &actorOptions{id: "actor-1", kind: "player"},
		scheduler: s,
	}
	s.actors.Store(actor.PID(), actor)
	const uid int64 = 1001
	s.relations[uid] = map[string]*Actor{actor.Kind(): actor}

	s.rw.Lock()
	removed := make(chan bool, 1)
	go func() {
		removed <- s.removeActor(actor)
	}()

	select {
	case got := <-removed:
		s.rw.Unlock()
		t.Fatalf("removeActor returned while relation write lock was held: removed=%v", got)
	case <-time.After(50 * time.Millisecond):
	}

	s.rw.Unlock()

	select {
	case got := <-removed:
		if !got {
			t.Fatal("removeActor returned false after relation lock was released")
		}
	case <-time.After(time.Second):
		t.Fatal("removeActor remained blocked after relation lock was released")
	}

	if _, ok := s.loadActor(uid, actor.Kind()); ok {
		t.Fatal("removed actor remained in scheduler relations")
	}
}
