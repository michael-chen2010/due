package node

import (
	"context"
	"testing"
	"time"

	"github.com/dobyte/due/v2/cluster"
	"github.com/dobyte/due/v2/packet"
	"github.com/dobyte/due/v2/session"
)

func TestProviderPassesTransportRequestMetadataIntoNodeContext(t *testing.T) {
	const route int32 = 9202
	deadline := time.UnixMilli(1790605000123)
	meta := cluster.RequestMetadata{
		Deadline:      deadline,
		CorrelationID: "corr-provider-123",
	}

	n := NewNode(WithID("provider-metadata-node"), WithName("game"))
	handled := make(chan struct{}, 1)
	n.router.AddRouteHandler(route, func(ctx Context) {
		if _, ok := ctx.Context().Deadline(); ok {
			t.Error("business request deadline must remain metadata until Game-side reconstruction")
		}
		got, ok := cluster.RequestMetadataFromContext(ctx.Context())
		if !ok {
			t.Error("handler Context is missing reconstructed request metadata")
		} else {
			if !got.Deadline.Equal(deadline) {
				t.Errorf("handler metadata deadline=%v, want %v", got.Deadline, deadline)
			}
			if got.CorrelationID != meta.CorrelationID {
				t.Errorf("handler correlation_id=%q, want %q", got.CorrelationID, meta.CorrelationID)
			}
		}

		ctx.Task(func(taskCtx Context) {
			taskMeta, taskOK := cluster.RequestMetadataFromContext(taskCtx.Context())
			if !taskOK || !taskMeta.Deadline.Equal(deadline) || taskMeta.CorrelationID != meta.CorrelationID {
				t.Errorf("Task metadata=%+v ok=%v, want %+v", taskMeta, taskOK, meta)
			}
			handled <- struct{}{}
		})
	})

	wire, err := packet.PackMessage(&packet.Message{Seq: 1, Route: route, Buffer: []byte("{}")})
	if err != nil {
		t.Fatal(err)
	}

	providerCtx := cluster.WithRequestMetadata(context.Background(), meta)
	p := &provider{node: n}
	if err := p.Deliver(
		providerCtx,
		"gate-1",
		"",
		11,
		22,
		session.Token{UID: 22, Generation: 7},
		wire,
	); err != nil {
		t.Fatal(err)
	}

	select {
	case req := <-n.router.receive():
		n.router.handle(req)
	case <-time.After(time.Second):
		t.Fatal("router did not receive provider request")
	}

	select {
	case <-handled:
	case <-time.After(time.Second):
		t.Fatal("handler was not invoked")
	}
}
