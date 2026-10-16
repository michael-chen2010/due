package link

import (
	"context"
	"testing"

	"github.com/dobyte/due/v2/cluster"
	"github.com/dobyte/due/v2/locate"
)

type gateHintLocator struct {
	hintTestLocator
	gate      string
	gateReads int
}

func (l *gateHintLocator) LocateGate(context.Context, int64) (string, error) {
	l.gateReads++
	return l.gate, nil
}
func TestGateHintsBoundedAndEventsOnlyInvalidate(t *testing.T) {
	ctx := context.Background()
	loc := &gateHintLocator{gate: "gate-new"}
	l := NewGateLinker(ctx, &Options{ID: "game-a", Kind: cluster.Node, Dispatch: cluster.Random, Locator: loc, SourceHintCapacity: 2})
	for i := int64(1); i < 100; i++ {
		if v, e := l.LocateGate(ctx, i); e != nil || v != "gate-new" {
			t.Fatal(e)
		}
	}
	if l.SourceHintCount() != 2 {
		t.Fatal("gate hint cache unbounded")
	}
	before := loc.gateReads
	if _, e := l.LocateGate(ctx, 1); e != nil || loc.gateReads != before+1 {
		t.Fatal("miss did not query authoritative Gate")
	}
	for i := 0; i < 3; i++ {
		l.applySourceEvent(&locate.Event{Type: locate.BindGate, UID: 1, InsID: "stale"})
		l.applySourceEvent(&locate.Event{Type: locate.UnbindGate, UID: 1, InsID: "gate-old"})
	}
	if v, e := l.LocateGate(ctx, 1); e != nil || v != "gate-new" || loc.gateReads != before+2 {
		t.Fatal("late notification poisoned fresh gate route")
	}
}
