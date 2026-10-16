package dispatcher

import (
	"github.com/dobyte/due/v2/cluster"
	"github.com/dobyte/due/v2/registry"
	"testing"
)

func TestEndpointCountEmptyAndReplacement(t *testing.T) {
	d := NewDispatcher(cluster.Random)
	if d.EndpointCount() != 0 {
		t.Fatal("empty dispatcher peers")
	}
	d.ReplaceServices(&registry.ServiceInstance{ID: "a", Name: "gate", Endpoint: "tcp://127.0.0.1:1234"}, &registry.ServiceInstance{ID: "b", Name: "gate", Endpoint: "tcp://127.0.0.1:1235"})
	if d.EndpointCount() != 2 {
		t.Fatal("discovered peer count missing")
	}
	d.ReplaceServices()
	if d.EndpointCount() != 0 {
		t.Fatal("empty replacement retained peer")
	}
}
