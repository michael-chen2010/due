package node

import (
	"strings"
	"testing"
	"time"

	"github.com/dobyte/due/v2/cluster"
)

func TestNodeShutdownStatusTracksWaitgroupAndStage(t *testing.T) {
	n := NewNode(WithID("shutdown-diagnostics-test"), WithName("game"))
	n.state.Store(int32(cluster.Work))
	n.addWait() // a deliberately unfinished accepted asynchronous operation

	done := make(chan struct{})
	go func() {
		n.Close()
		close(done)
	}()

	deadline := time.Now().Add(time.Second)
	for !strings.Contains(n.ShutdownCloseStatus(), "stage=waitgroup") {
		if time.Now().After(deadline) {
			n.doneWait()
			<-done
			t.Fatal("Node.Close did not reach waitgroup stage")
		}
		time.Sleep(time.Millisecond)
	}
	if status := n.ShutdownCloseStatus(); !strings.Contains(status, "pending_waits=1") {
		n.doneWait()
		<-done
		t.Fatalf("pending work not reflected in shutdown diagnostic: %s", status)
	}
	select {
	case <-done:
		t.Fatal("Close must wait for accepted asynchronous operation")
	default:
	}
	n.doneWait()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close did not complete after pending work was released")
	}
	if status := n.ShutdownCloseStatus(); status != "stage=complete pending_waits=0" {
		t.Fatalf("completed Close diagnostic=%s", status)
	}
}

func TestNodeShutdownStatusBeforeClose(t *testing.T) {
	n := NewNode(WithID("shutdown-diagnostics-test"), WithName("game"))
	if got := n.ShutdownCloseStatus(); got != "stage=not-started pending_waits=0" {
		t.Fatalf("initial shutdown status=%s", got)
	}
}
