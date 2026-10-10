package node

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dobyte/due/v2/cluster"
	"github.com/dobyte/due/v2/locate"
)

// A persisted owner binding is not an accepted in-flight request. The Game
// releases its route only after Final Flush during the Destroy lifecycle.
type shutdownOwnerLocator struct{ locate.Locator }

func (*shutdownOwnerLocator) BindNode(context.Context, int64, string, string) error   { return nil }
func (*shutdownOwnerLocator) UnbindNode(context.Context, int64, string, string) error { return nil }

func TestNodeCloseDoesNotDeadlockOnOwnerReleasedDuringDestroy(t *testing.T) {
	n := NewNode(WithID("shutdown-owner-node"), WithName("game"), WithLocator(&shutdownOwnerLocator{}))
	n.state.Store(int32(cluster.Work))
	const playerID int64 = 123456
	if err := n.Proxy().BindNode(context.Background(), playerID); err != nil {
		t.Fatalf("bind persisted owner: %v", err)
	}

	closed := make(chan struct{})
	go func() {
		n.Close()
		close(closed)
	}()

	select {
	case <-closed:
	case <-time.After(250 * time.Millisecond):
		status := n.ShutdownCloseStatus()
		// Unblock the test's Close goroutine on a RED failure; never leak it
		// into subsequent cases.
		_ = n.Proxy().UnbindNode(context.Background(), playerID)
		<-closed
		if !strings.Contains(status, "stage=waitgroup") {
			t.Fatalf("Close blocked outside of expected waitgroup stage: %s", status)
		}
		t.Fatalf("Close blocked waiting for an owner only released in Destroy: %s", status)
	}

	if status := n.ShutdownCloseStatus(); status != "stage=complete pending_waits=0 owned_sources=1" {
		t.Fatalf("close status must separate in-flight operations from persisted owners: %s", status)
	}

	// Closing must not silently revoke persisted ownership: the Game Destroy
	// hook performs Final Flush before it calls UnbindNode.
	if nid, err := n.Proxy().LocateNode(context.Background(), playerID, "game"); err != nil || nid != "shutdown-owner-node" {
		t.Fatalf("Close revoked owner before Final Flush: node=%q err=%v", nid, err)
	}
	// Destroy transitions the node to Shut before it invokes the Game's
	// FinalFlush -> DrainRelease hook. The source diagnostic must still drain.
	n.state.Store(int32(cluster.Shut))
	if err := n.Proxy().UnbindNode(context.Background(), playerID); err != nil {
		t.Fatalf("Destroy-time ownership release: %v", err)
	}
	if status := n.ShutdownCloseStatus(); status != "stage=complete pending_waits=0" {
		t.Fatalf("ownership release after transition to Shut left stale source: %s", status)
	}
}
