package link

import (
	"context"
	"testing"

	"github.com/dobyte/due/v2/cluster"
	"github.com/dobyte/due/v2/core/endpoint"
	"github.com/dobyte/due/v2/registry"
)

func TestResolveStatelessAffinityNode(t *testing.T) {
	const route int32 = 11001
	locator := &staleRouteTestLocator{node: "game-a"}
	linker := NewNodeLinker(context.Background(), &Options{Kind: cluster.Gate, Locator: locator, Dispatch: cluster.Random})
	service := func(id string, routeID int32, state cluster.State) *registry.ServiceInstance {
		return &registry.ServiceInstance{
			ID: id, Name: "game", Alias: "game", Kind: cluster.Node.String(),
			State: state.String(), Endpoint: endpoint.NewEndpoint("grpc", "127.0.0.1:4001", false).String(),
			Routes: []registry.Route{{ID: routeID, Stateful: false, Authorized: false}},
		}
	}
	linker.dispatcher.ReplaceServices(service("game-a", route, cluster.Work), service("game-b", route, cluster.Work))
	for _, tc := range []struct {
		name  string
		uid   int64
		group string
		owner string
		want  string
	}{
		{"existing owner", 42, "game", "game-a", "game-a"},
		{"missing owner", 42, "game", "", ""},
		{"stale endpoint", 42, "game", "game-gone", ""},
		{"wrong group", 42, "other", "game-a", ""},
		{"invalid uid", 0, "game", "game-a", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			locator.node = tc.owner
			got, err := linker.ResolveStatelessAffinityNode(context.Background(), route, tc.uid, tc.group)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("target=%q, want %q", got, tc.want)
			}
		})
	}
}
