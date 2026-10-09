package link

import (
	"context"
	stderrors "errors"
	"testing"

	"github.com/dobyte/due/v2/cluster"
	"github.com/dobyte/due/v2/core/endpoint"
	"github.com/dobyte/due/v2/registry"
)

type failingAffinityLocator struct{ *staleRouteTestLocator }

func (*failingAffinityLocator) LocateNode(context.Context, int64, string) (string, error) {
	return "", stderrors.New("locator unavailable")
}

func TestStatelessAffinityNeverRedirectsAuthenticatedRoutesOrHidesLookupFailure(t *testing.T) {
	const (
		login      int32 = 11001
		stateful   int32 = 11002
		authorized int32 = 11003
	)
	makeLinker := func(locator *staleRouteTestLocator) *NodeLinker {
		l := NewNodeLinker(context.Background(), &Options{Kind: cluster.Gate, Locator: locator, Dispatch: cluster.Random})
		l.dispatcher.ReplaceServices(&registry.ServiceInstance{
			ID: "game-a", Name: "game", Alias: "game", Kind: cluster.Node.String(),
			State: cluster.Work.String(), Endpoint: endpoint.NewEndpoint("grpc", "127.0.0.1:4001", false).String(),
			Routes: []registry.Route{
				{ID: login, Stateful: false, Authorized: false},
				{ID: stateful, Stateful: true, Authorized: true},
				{ID: authorized, Stateful: false, Authorized: true},
			},
		})
		return l
	}
	locator := &staleRouteTestLocator{node: "game-a"}
	l := makeLinker(locator)
	// This direct lookup must bypass stale cached stateful sources.
	l.doStoreSource(42, "game", "game-b")
	nid, err := l.ResolveStatelessAffinityNode(context.Background(), login, 42, "game")
	if err != nil || nid != "game-a" {
		t.Fatalf("fresh owner: %q %v", nid, err)
	}
	for _, route := range []int32{stateful, authorized} {
		nid, err = l.ResolveStatelessAffinityNode(context.Background(), route, 42, "game")
		if err != nil || nid != "" {
			t.Fatalf("protected route %d target=%q error=%v", route, nid, err)
		}
	}
	failed := makeLinker(locator)
	failed.opts.Locator = &failingAffinityLocator{locator}
	if nid, err = failed.ResolveStatelessAffinityNode(context.Background(), login, 42, "game"); err == nil || nid != "" {
		t.Fatalf("locator failure must not silently randomize: %q %v", nid, err)
	}
}
