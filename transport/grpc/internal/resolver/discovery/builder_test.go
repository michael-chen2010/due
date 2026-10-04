package discovery

import (
	"testing"

	"github.com/dobyte/due/v2/registry"
)

func TestUpdateStatesKeepsAllEndpointsForSameService(t *testing.T) {
	builder := NewBuilder()
	builder.UpdateStates([]*registry.ServiceInstance{
		{
			ID:       "rank-1",
			Endpoint: "grpc://127.0.0.1:39001?is_secure=false",
			Services: []string{"rank"},
		},
		{
			ID:       "rank-2",
			Endpoint: "grpc://127.0.0.1:39002?is_secure=false",
			Services: []string{"rank"},
		},
	})

	state, ok := builder.states["rank"]
	if !ok {
		t.Fatal("rank resolver state missing")
	}
	if got := len(state.Addresses); got != 2 {
		t.Fatalf(
			"rank resolver addresses=%d want=2 state=%+v",
			got,
			state,
		)
	}

	got := map[string]bool{}
	for _, address := range state.Addresses {
		got[address.Addr] = true
	}
	for _, want := range []string{
		"127.0.0.1:39001",
		"127.0.0.1:39002",
	} {
		if !got[want] {
			t.Fatalf(
				"rank resolver addresses=%v missing=%q",
				got,
				want,
			)
		}
	}
}
