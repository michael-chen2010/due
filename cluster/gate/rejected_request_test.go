package gate

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dobyte/due/v2/cluster"
	dueerrors "github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/locate"
	"github.com/dobyte/due/v2/network"
	"github.com/dobyte/due/v2/packet"
	"github.com/dobyte/due/v2/registry"
)

func TestRejectedRequestPreservesFrameConnectionAndMetadata(t *testing.T) {
	for _, callbackFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "response", true: "callback_failure"}[callbackFailure], func(t *testing.T) {
			conn := &ownershipTestConn{id: 71}
			frame, err := packet.PackMessage(&packet.Message{Seq: 8, Route: 9301, Buffer: []byte("payload")})
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			var traced error
			g := NewGate(WithCorrelationIDGenerator(func() string { return "rejected-correlation" }), WithTraceContextGenerator(func(cluster.RequestMetadata) (string, string, func(error)) {
				return "", "", func(e error) { traced = e }
			}), WithRejectedRequestHandler(func(got network.Conn, metadata cluster.RequestMetadata, data []byte, cause error) error {
				calls++
				if got != conn || metadata.CorrelationID != "rejected-correlation" || string(data) != string(frame) || !errors.Is(cause, dueerrors.ErrNotFoundRoute) {
					t.Errorf("rejection got=%v metadata=%+v cause=%v", got, metadata, cause)
				}
				if callbackFailure {
					return errors.New("response failed")
				}
				return nil
			}))
			defer g.cancel()
			g.session.AddConn(conn)
			g.handleReceive(conn, frame)
			if calls != 1 || !errors.Is(traced, dueerrors.ErrNotFoundRoute) {
				t.Fatalf("calls=%d traced=%v", calls, traced)
			}
		})
	}
}

type rejectedMissingLocator struct{ locate.Locator }

func (*rejectedMissingLocator) LocateNode(context.Context, int64, string) (string, error) {
	return "", nil
}

func TestRejectedRequestAuthenticatedMissingLocation(t *testing.T) {
	w := &rejectedWatcher{updates: make(chan []*registry.ServiceInstance, 1), stopped: make(chan struct{})}
	calls := 0
	g := NewGate(WithRegistry(&rejectedRegistry{watcher: w}), WithLocator(&rejectedMissingLocator{}), WithRejectedRequestHandler(func(_ network.Conn, _ cluster.RequestMetadata, _ []byte, cause error) error {
		calls++
		if !errors.Is(cause, dueerrors.ErrNotFoundUserLocation) {
			t.Errorf("cause=%v", cause)
		}
		return nil
	}))
	w.ctx = g.ctx
	g.proxy.nodeLinker.WatchClusterInstance()
	defer func() {
		g.cancel()
		select {
		case <-w.stopped:
		case <-time.After(time.Second):
			t.Error("watcher not stopped")
		}
	}()
	w.updates <- []*registry.ServiceInstance{{ID: "live-game", Name: "game", Kind: cluster.Node.String(), State: cluster.Work.String(), Endpoint: "tcp://127.0.0.1:1", Routes: []registry.Route{{ID: 9301, Stateful: true, Authorized: true}}}}
	deadline := time.Now().Add(time.Second)
	for !g.proxy.nodeLinker.HasNode("live-game") && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !g.proxy.nodeLinker.HasNode("live-game") {
		t.Fatal("controlled live route not installed")
	}
	conn := &ownershipTestConn{id: 74}
	g.session.AddConn(conn)
	if e := g.session.Bind(74, 42); e != nil {
		t.Fatal(e)
	}
	frame, e := packet.PackMessage(&packet.Message{Seq: 1, Route: 9301})
	if e != nil {
		t.Fatal(e)
	}
	g.handleReceive(conn, frame)
	if calls != 1 {
		t.Fatalf("missing player location was silently accepted: rejected callbacks=%d want1", calls)
	}
}

type rejectedRegistry struct {
	registry.Registry
	watcher *rejectedWatcher
}

func (r *rejectedRegistry) Watch(context.Context, string) (registry.Watcher, error) {
	return r.watcher, nil
}

type rejectedWatcher struct {
	ctx     context.Context
	updates chan []*registry.ServiceInstance
	stopped chan struct{}
}

func (w *rejectedWatcher) Next() ([]*registry.ServiceInstance, error) {
	select {
	case <-w.ctx.Done():
		return nil, w.ctx.Err()
	case services := <-w.updates:
		return services, nil
	}
}
func (w *rejectedWatcher) Stop() error { close(w.stopped); return nil }

func TestRejectedRequestRouteWithoutAvailableEndpoint(t *testing.T) {
	w := &rejectedWatcher{updates: make(chan []*registry.ServiceInstance, 1), stopped: make(chan struct{})}
	reg := &rejectedRegistry{watcher: w}
	calls := 0
	g := NewGate(WithRegistry(reg), WithRejectedRequestHandler(func(_ network.Conn, _ cluster.RequestMetadata, _ []byte, cause error) error {
		calls++
		if !errors.Is(cause, dueerrors.ErrNotFoundEndpoint) {
			t.Errorf("cause=%v", cause)
		}
		return nil
	}))
	w.ctx = g.ctx
	g.proxy.nodeLinker.WatchClusterInstance()
	defer func() {
		g.cancel()
		select {
		case <-w.stopped:
		case <-time.After(time.Second):
			t.Error("watcher not stopped")
		}
	}()
	w.updates <- []*registry.ServiceInstance{{ID: "shut-game", Name: "game", Kind: cluster.Node.String(), State: cluster.Shut.String(), Endpoint: "tcp://127.0.0.1:1", Routes: []registry.Route{{ID: 9301}}}}
	deadline := time.Now().Add(time.Second)
	for !g.proxy.nodeLinker.HasNode("shut-game") && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !g.proxy.nodeLinker.HasNode("shut-game") {
		t.Fatal("controlled registry snapshot not installed")
	}
	frame, e := packet.PackMessage(&packet.Message{Seq: 1, Route: 9301})
	if e != nil {
		t.Fatal(e)
	}
	conn := &ownershipTestConn{id: 73}
	g.session.AddConn(conn)
	g.handleReceive(conn, frame)
	if calls != 1 {
		t.Fatalf("calls=%d", calls)
	}
}

func TestRejectedRequestDefaultDisabled(t *testing.T) {
	g := NewGate()
	defer g.cancel()
	if g.opts.rejectedRequestHandler != nil {
		t.Fatal("rejection callback must be opt-in")
	}
	frame, e := packet.PackMessage(&packet.Message{Seq: 1, Route: 9301})
	if e != nil {
		t.Fatal(e)
	}
	conn := &ownershipTestConn{id: 72}
	g.session.AddConn(conn)
	g.handleReceive(conn, frame)
}
