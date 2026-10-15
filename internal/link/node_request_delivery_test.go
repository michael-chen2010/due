package link

import (
	"context"
	"errors"
	"testing"

	"github.com/dobyte/due/v2/cluster"
	dueerrors "github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/registry"
)

type requestDeliveryBuffer struct{ releases int }

func (*requestDeliveryBuffer) Len() int      { return 1 }
func (*requestDeliveryBuffer) Bytes() []byte { return []byte{1} }
func (b *requestDeliveryBuffer) Release()    { b.releases++ }

func TestDeliverRequestMissingLocationRejectsAndReleasesBuffer(t *testing.T) {
	for _, strict := range []bool{false, true} {
		l := NewNodeLinker(context.Background(), &Options{Kind: cluster.Gate, Locator: &staleRouteTestLocator{}})
		l.dispatcher.ReplaceServices(&registry.ServiceInstance{ID: "live", Name: "game", State: cluster.Work.String(), Endpoint: "tcp://127.0.0.1:1", Routes: []registry.Route{{ID: 99, Stateful: true, Authorized: true}}})
		buf := &requestDeliveryBuffer{}
		args := &DeliverArgs{UID: 42, Route: 99, Buffer: buf}
		var err error
		if strict {
			err = l.DeliverRequest(context.Background(), args)
		} else {
			err = l.Deliver(context.Background(), args)
		}
		if (strict && !errors.Is(err, dueerrors.ErrNotFoundUserLocation)) || (!strict && err != nil) {
			t.Fatalf("strict=%t err=%v", strict, err)
		}
		if buf.releases != 1 {
			t.Fatalf("strict=%t buffer releases=%d want1", strict, buf.releases)
		}
	}
}
