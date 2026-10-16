package link

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/dobyte/due/v2/cluster"
	"github.com/dobyte/due/v2/locate"
)

type hintTestLocator struct {
	staleRouteTestLocator
	reads                int
	failBind, failUnbind bool
}

func (l *hintTestLocator) LocateNode(ctx context.Context, id int64, name string) (string, error) {
	l.reads++
	return l.node, nil
}
func (l *hintTestLocator) BindNode(ctx context.Context, id int64, name, node string) error {
	if l.failBind {
		return errors.New("unknown bind")
	}
	return nil
}
func (l *hintTestLocator) UnbindNode(ctx context.Context, id int64, name, node string) error {
	if l.failUnbind {
		return errors.New("unknown unbind")
	}
	return nil
}

func TestNodeHintsEvictRequeryAndNeverForgetOwnedACK(t *testing.T) {
	ctx := context.Background()
	loc := &hintTestLocator{staleRouteTestLocator: staleRouteTestLocator{node: "game-a"}}
	waits, dones := 0, 0
	l := NewNodeLinker(ctx, &Options{ID: "game-a", Kind: cluster.Node, Dispatch: cluster.Random, Locator: loc, SourceHintCapacity: 2, WaitHandler: func() { waits++ }, DoneHandler: func() { dones++ }})
	// An authoritative self lookup and arbitrary missing notifications cannot
	// create owned duties; only the actual local Bind ACK can do so.
	for id := int64(1); id < 100; id++ {
		if _, e := l.LocateNode(ctx, id, "game"); e != nil {
			t.Fatal(e)
		}
	}
	if len(l.LocalBoundUserIDs("game", "game-a")) != 0 || waits != 0 || l.SourceHintCount() != 2 {
		t.Fatal("hints became unbounded owned duties")
	}
	for id := int64(1); id < 1000; id++ {
		l.applySourceEvent(&locate.Event{Type: locate.BindNode, UID: id, InsName: "game", InsID: "game-a"})
	}
	if waits != 0 || len(l.LocalBoundUserIDs("game", "game-a")) != 0 {
		t.Fatal("self notification created unadmitted owned duty")
	}
	if e := l.BindNode(ctx, 100, "game", "game-a"); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 3; i++ {
		l.applySourceEvent(&locate.Event{Type: locate.BindNode, UID: 100, InsName: "game", InsID: "new-owner"})
		l.applySourceEvent(&locate.Event{Type: locate.UnbindNode, UID: 100, InsName: "game", InsID: "game-a"})
	}
	if len(l.LocalBoundUserIDs("game", "game-a")) != 1 || waits != 1 || dones != 0 {
		t.Fatal("notification changed owned responsibility")
	}
	loc.failUnbind = true
	if e := l.UnbindNode(ctx, 100, "game", "game-a"); e == nil {
		t.Fatal("unknown unbind accepted")
	}
	if len(l.LocalBoundUserIDs("game", "game-a")) != 1 {
		t.Fatal("unknown unbind lost responsibility")
	}
	loc.failUnbind = false
	if e := l.UnbindNode(ctx, 100, "game", "game-a"); e != nil {
		t.Fatal(e)
	}
	if len(l.LocalBoundUserIDs("game", "game-a")) != 0 || dones != 1 {
		t.Fatal("actual ACK did not release exact duty")
	}
	loc.failBind = true
	if e := l.BindNode(ctx, 101, "game", "game-a"); e == nil || waits != 1 {
		t.Fatal("unknown bind fabricated local ACK")
	}
	loc.node = "remote"
	before := loc.reads
	if v, e := l.LocateNode(ctx, 1, "game"); e != nil || v != "remote" || loc.reads != before+1 {
		t.Fatal("evicted hint did not query authority")
	}
}

type blockingHintLocator struct {
	hintTestLocator
	first            atomic.Bool
	started, release chan struct{}
}

func (l *blockingHintLocator) LocateNode(context.Context, int64, string) (string, error) {
	if l.first.CompareAndSwap(false, true) {
		close(l.started)
		<-l.release
		return "old", nil
	}
	return "new", nil
}
func TestNodeHintsLateAuthorityReplyCannotRefillAfterNotification(t *testing.T) {
	ctx := context.Background()
	loc := &blockingHintLocator{started: make(chan struct{}), release: make(chan struct{})}
	l := NewNodeLinker(ctx, &Options{ID: "game-a", Kind: cluster.Node, Dispatch: cluster.Random, Locator: loc, SourceHintCapacity: 2})
	done := make(chan error, 1)
	go func() { _, e := l.LocateNode(ctx, 1, "game"); done <- e }()
	<-loc.started
	l.applySourceEvent(&locate.Event{Type: locate.BindNode, UID: 1, InsName: "game", InsID: "new"})
	close(loc.release)
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	if l.SourceHintCount() != 0 {
		t.Fatal("old in-flight lookup filled after notification")
	}
	if v, e := l.LocateNode(ctx, 1, "game"); e != nil || v != "new" {
		t.Fatal("next miss did not requery new authority")
	}
}
