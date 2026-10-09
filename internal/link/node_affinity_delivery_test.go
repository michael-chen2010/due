package link

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/dobyte/due/v2/cluster"
	"github.com/dobyte/due/v2/core/endpoint"
	transportnode "github.com/dobyte/due/v2/internal/transporter/node"
	"github.com/dobyte/due/v2/packet"
	"github.com/dobyte/due/v2/registry"
	"github.com/dobyte/due/v2/session"
)

type affinityDelivery struct {
	uid   int64
	token session.Token
}
type affinityDeliveryProvider struct{ got chan affinityDelivery }

func (*affinityDeliveryProvider) Trigger(context.Context, string, int64, int64, session.Token, cluster.Event) error {
	return nil
}
func (p *affinityDeliveryProvider) Deliver(_ context.Context, _, _ string, _, uid int64, token session.Token, _ []byte) error {
	p.got <- affinityDelivery{uid: uid, token: token}
	return nil
}
func (*affinityDeliveryProvider) GetState() (cluster.State, error) { return cluster.Work, nil }
func (*affinityDeliveryProvider) SetState(cluster.State) error     { return nil }

func startAffinityNode(t *testing.T, p *affinityDeliveryProvider) string {
	t.Helper()
	server, err := transportnode.NewServer(p, &transportnode.ServerOptions{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	stopped := make(chan error, 1)
	go func() { stopped <- server.Start() }()
	t.Cleanup(func() {
		if err := server.Stop(); err != nil {
			t.Errorf("stop node: %v", err)
		}
		if err := <-stopped; err != nil {
			t.Errorf("node exited: %v", err)
		}
	})
	deadline := time.Now().Add(2 * time.Second)
	for {
		c, err := net.DialTimeout("tcp", server.ListenAddr(), 30*time.Millisecond)
		if err == nil {
			_ = c.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("node not listening: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return server.ListenAddr()
}

func TestUnauthenticatedLoginAffinityDeliversOnlyToExistingOwner(t *testing.T) {
	const route int32 = 11001
	owner := &affinityDeliveryProvider{got: make(chan affinityDelivery, 2)}
	other := &affinityDeliveryProvider{got: make(chan affinityDelivery, 2)}
	ownerAddr := startAffinityNode(t, owner)
	otherAddr := startAffinityNode(t, other)
	locator := &staleRouteTestLocator{node: "game-a"}
	linker := NewNodeLinker(context.Background(), &Options{
		ID: "owner-affinity-gate", Kind: cluster.Gate, Locator: locator,
		Dispatch: cluster.Random, ConnNum: 1, CallTimeout: time.Second,
		DialTimeout: time.Second, DialRetryTimes: 1, WriteTimeout: time.Second,
		WriteQueueSize: 64, FaultRecoveryTime: time.Second,
	})
	makeService := func(id, addr string) *registry.ServiceInstance {
		return &registry.ServiceInstance{
			ID: id, Name: "game", Alias: "game", Kind: cluster.Node.String(),
			State:    cluster.Work.String(),
			Endpoint: endpoint.NewEndpoint("grpc", addr, false).String(),
			Routes:   []registry.Route{{ID: route, Stateful: false, Authorized: false}},
		}
	}
	linker.dispatcher.ReplaceServices(makeService("game-a", ownerAddr), makeService("game-b", otherAddr))
	nid, err := linker.ResolveStatelessAffinityNode(context.Background(), route, 8810002100, "game")
	if err != nil {
		t.Fatal(err)
	}
	if nid != "game-a" {
		t.Fatalf("owner route = %q, want game-a", nid)
	}
	data, err := packet.PackMessage(&packet.Message{Seq: 1, Route: route, Buffer: []byte("login")})
	if err != nil {
		t.Fatal(err)
	}
	if err = linker.Deliver(context.Background(), &DeliverArgs{NID: nid, CID: 9, UID: 0, Route: route, Buffer: data}); err != nil {
		t.Fatal(err)
	}
	select {
	case delivery := <-owner.got:
		if delivery.uid != 0 || delivery.token.UID != 0 || delivery.token.Generation != 0 {
			t.Fatalf("preauth delivery must retain UID=0, got %+v", delivery)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("owner Game did not receive Login")
	}
	select {
	case <-other.got:
		t.Fatal("non-owner Game received Login")
	case <-time.After(100 * time.Millisecond):
	}
}
