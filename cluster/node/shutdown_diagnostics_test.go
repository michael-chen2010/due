package node

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dobyte/due/v2/cluster"
	"github.com/dobyte/due/v2/registry"
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

type canceledHeartbeatRegistry struct {
	registrarCtx context.Context
	heartbeats   chan struct{}
}

func (r *canceledHeartbeatRegistry) Name() string { return "isolated-heartbeat-registry" }
func (r *canceledHeartbeatRegistry) Register(ctx context.Context, _ *registry.ServiceInstance) error {
	// Emulate the registrar's cancellation-aware heartbeat send. The channel
	// intentionally has no receiver, representing a stopped worker.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.registrarCtx.Done():
		return r.registrarCtx.Err()
	case r.heartbeats <- struct{}{}:
		return nil
	}
}
func (*canceledHeartbeatRegistry) Deregister(context.Context, *registry.ServiceInstance) error {
	return nil
}
func (*canceledHeartbeatRegistry) Watch(context.Context, string) (registry.Watcher, error) {
	return nil, errors.New("unused in isolated close test")
}
func (*canceledHeartbeatRegistry) Services(context.Context, string) ([]*registry.ServiceInstance, error) {
	return nil, nil
}

func TestNodeCloseCompletesWhenRegistrarWasAlreadyCanceled(t *testing.T) {
	registrarCtx, cancel := context.WithCancel(context.Background())
	reg := &canceledHeartbeatRegistry{
		registrarCtx: registrarCtx,
		heartbeats:   make(chan struct{}),
	}
	n := NewNode(WithID("close-canceled-registry"), WithName("game"), WithRegistry(reg))
	n.instances = []*registry.ServiceInstance{{
		ID: "isolated-game", Name: "game", State: cluster.Work.String(),
	}}
	closeHookCalled := make(chan struct{})
	n.Proxy().AddHookListener(cluster.Close, func(*Proxy) { close(closeHookCalled) })
	n.state.Store(int32(cluster.Work))
	cancel() // Registrar's heartbeat receiver already exited.
	done := make(chan struct{})
	go func() {
		n.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("Node.Close stuck in canceled registry refresh: %s", n.ShutdownCloseStatus())
	}
	select {
	case <-closeHookCalled:
	default:
		t.Fatal("Node.Close skipped required close hook after canceled registry refresh")
	}
	if status := n.ShutdownCloseStatus(); status != "stage=complete pending_waits=0" {
		t.Fatalf("Node.Close final status=%s", status)
	}
}
