package link

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/dobyte/due/v2/cluster"
	"github.com/dobyte/due/v2/core/endpoint"
	transportnode "github.com/dobyte/due/v2/internal/transporter/node"
	"github.com/dobyte/due/v2/locate"
	"github.com/dobyte/due/v2/registry"
	"github.com/dobyte/due/v2/session"
)

type staleRouteTestLocator struct {
	node string
}

func (*staleRouteTestLocator) Name() string { return "stale-route-test" }

func (*staleRouteTestLocator) Watch(context.Context, ...string) (locate.Watcher, error) {
	return nil, errors.New("watch not used")
}

func (*staleRouteTestLocator) BindGate(context.Context, int64, string) error {
	return nil
}

func (l *staleRouteTestLocator) BindNode(
	_ context.Context,
	_ int64,
	_ string,
	nid string,
) error {
	l.node = nid
	return nil
}

func (*staleRouteTestLocator) UnbindGate(context.Context, int64, string) error {
	return nil
}

func (*staleRouteTestLocator) UnbindNode(
	context.Context,
	int64,
	string,
	string,
) error {
	return nil
}

func (*staleRouteTestLocator) LocateGate(context.Context, int64) (string, error) {
	return "", nil
}

func (l *staleRouteTestLocator) LocateNode(
	context.Context,
	int64,
	string,
) (string, error) {
	return l.node, nil
}

func (l *staleRouteTestLocator) LocateNodes(
	context.Context,
	int64,
) (map[string]string, error) {
	return map[string]string{"game": l.node}, nil
}

func TestNodeLinkerDeliverRetriesStatefulRouteAfterStaleCachedNodeFails(t *testing.T) {
	provider := &linkerMetadataProvider{got: make(chan cluster.RequestMetadata, 1)}
	server, err := transportnode.NewServer(
		provider,
		&transportnode.ServerOptions{Addr: "127.0.0.1:0"},
	)
	if err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- server.Start() }()
	t.Cleanup(func() {
		if err := server.Stop(); err != nil {
			t.Errorf("stop live node server: %v", err)
		}
		if err := <-errCh; err != nil {
			t.Errorf("live node server exited: %v", err)
		}
	})

	listenDeadline := time.Now().Add(time.Second)
	for {
		conn, dialErr := net.DialTimeout("tcp", server.ListenAddr(), 20*time.Millisecond)
		if dialErr == nil {
			_ = conn.Close()
			break
		}
		if time.Now().After(listenDeadline) {
			t.Fatalf("live node server did not listen: %v", dialErr)
		}
		time.Sleep(10 * time.Millisecond)
	}

	deadListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddr := deadListener.Addr().String()
	if err := deadListener.Close(); err != nil {
		t.Fatal(err)
	}

	const (
		uid   = int64(42)
		route = int32(1020)
	)
	locator := &staleRouteTestLocator{node: "new-node"}
	linker := NewNodeLinker(context.Background(), &Options{
		ID:                "gate-stale-route",
		Kind:              cluster.Gate,
		Locator:           locator,
		Dispatch:          cluster.Random,
		ConnNum:           1,
		CallTimeout:       time.Second,
		DialTimeout:       50 * time.Millisecond,
		DialRetryTimes:    1,
		WriteTimeout:      time.Second,
		WriteQueueSize:    64,
		FaultRecoveryTime: time.Second,
	})
	linker.dispatcher.ReplaceServices(
		&registry.ServiceInstance{
			ID:       "old-node",
			Name:     "game",
			Kind:     cluster.Node.String(),
			Alias:    "game",
			State:    cluster.Work.String(),
			Endpoint: endpoint.NewEndpoint("grpc", deadAddr, false).String(),
			Routes: []registry.Route{{
				ID:         route,
				Stateful:   true,
				Authorized: true,
			}},
		},
		&registry.ServiceInstance{
			ID:       "new-node",
			Name:     "game",
			Kind:     cluster.Node.String(),
			Alias:    "game",
			State:    cluster.Work.String(),
			Endpoint: endpoint.NewEndpoint("grpc", server.ListenAddr(), false).String(),
			Routes: []registry.Route{{
				ID:         route,
				Stateful:   true,
				Authorized: true,
			}},
		},
	)

	// Simulate the exact failover race: Redis/locator already points to the
	// surviving Game, but this Gate has not consumed the BindNode watch event
	// yet and still holds the failed Game in its local source cache.
	linker.doStoreSource(uid, "game", "old-node")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	meta := cluster.RequestMetadata{CorrelationID: "corr-stale-route"}
	err = linker.Deliver(ctx, &DeliverArgs{
		CID:      7,
		UID:      uid,
		Token:    session.Token{UID: uid, Generation: 3},
		Metadata: meta,
		Route:    route,
		Buffer:   []byte("hello"),
	})
	if err != nil {
		t.Fatalf("Deliver after stale cached node: %v", err)
	}

	select {
	case got := <-provider.got:
		if got.CorrelationID != meta.CorrelationID {
			t.Fatalf("correlation_id=%q, want %q", got.CorrelationID, meta.CorrelationID)
		}
	case <-time.After(time.Second):
		t.Fatal("surviving node did not receive retried delivery")
	}

	if got, ok := linker.doLoadSource(uid, "game"); !ok || got != "new-node" {
		t.Fatalf("cached source=%q ok=%v, want new-node/true", got, ok)
	}
}
