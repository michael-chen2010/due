package link

import (
	"context"
	"reflect"
	"slices"
	"testing"

	"github.com/dobyte/due/v2/cluster"
)

func TestNodeLinkerLocalBoundUserIDsTracksOnlyExactLocalNode(t *testing.T) {
	l := NewNodeLinker(context.Background(), &Options{
		ID: "game-a", Kind: cluster.Node, Dispatch: cluster.Random,
	})
	l.doStoreSource(42, "game", "game-a")
	l.doStoreSource(88, "game", "game-a")
	l.doStoreSource(77, "game", "game-b")
	l.doStoreSource(100, "chat", "game-a")
	got := l.LocalBoundUserIDs("game", "game-a")
	slices.Sort(got)
	if !reflect.DeepEqual(got, []int64{42, 88}) {
		t.Fatalf("local owner snapshot=%v, want [42 88]", got)
	}
	// Snapshot must not alias the internal map and must filter owner changes.
	got[0] = -123
	l.doStoreSource(42, "game", "game-b")
	got = l.LocalBoundUserIDs("game", "game-a")
	if !reflect.DeepEqual(got, []int64{88}) {
		t.Fatalf("source reassignment should exclude old local owner, got %v", got)
	}
	l.doDeleteSource(88, "game", "game-a")
	if ids := l.LocalBoundUserIDs("game", "game-a"); len(ids) != 0 {
		t.Fatalf("unbound source still appears as local owner: %v", ids)
	}
}
