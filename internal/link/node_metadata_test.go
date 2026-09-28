package link

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/dobyte/due/v2/cluster"
	"github.com/dobyte/due/v2/core/endpoint"
	transportnode "github.com/dobyte/due/v2/internal/transporter/node"
	"github.com/dobyte/due/v2/registry"
	"github.com/dobyte/due/v2/session"
)

type linkerMetadataProvider struct {
	got chan cluster.RequestMetadata
}

func (p *linkerMetadataProvider) Trigger(context.Context, string, int64, int64, session.Token, cluster.Event) error {
	return nil
}

func (p *linkerMetadataProvider) Deliver(ctx context.Context, _, _ string, _, _ int64, _ session.Token, _ []byte) error {
	meta, _ := cluster.RequestMetadataFromContext(ctx)
	p.got <- meta
	return nil
}

func (p *linkerMetadataProvider) GetState() (cluster.State, error) {
	return cluster.Work, nil
}

func (p *linkerMetadataProvider) SetState(cluster.State) error {
	return nil
}

func TestNodeLinkerDeliverForwardsRequestMetadata(t *testing.T) {
	provider := &linkerMetadataProvider{got: make(chan cluster.RequestMetadata, 1)}
	server, err := transportnode.NewServer(provider, &transportnode.ServerOptions{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- server.Start() }()
	t.Cleanup(func() {
		if err := server.Stop(); err != nil {
			t.Errorf("stop linker metadata server: %v", err)
		}
		if err := <-errCh; err != nil {
			t.Errorf("linker metadata server exited: %v", err)
		}
	})

	listenDeadline := time.Now().Add(3 * time.Second)
	for {
		conn, dialErr := net.DialTimeout("tcp", server.ListenAddr(), 50*time.Millisecond)
		if dialErr == nil {
			_ = conn.Close()
			break
		}
		if time.Now().After(listenDeadline) {
			t.Fatalf("linker metadata server did not listen: %v", dialErr)
		}
		time.Sleep(10 * time.Millisecond)
	}

	linker := NewNodeLinker(context.Background(), &Options{
		ID:                "gate-link-metadata",
		Kind:              cluster.Gate,
		Dispatch:          cluster.Random,
		ConnNum:           1,
		CallTimeout:       time.Second,
		DialTimeout:       time.Second,
		DialRetryTimes:    1,
		WriteTimeout:      time.Second,
		WriteQueueSize:    64,
		FaultRecoveryTime: time.Second,
	})
	linker.dispatcher.ReplaceServices(&registry.ServiceInstance{
		ID:       "node-metadata",
		Name:     "game",
		Kind:     cluster.Node.String(),
		Alias:    "game",
		State:    cluster.Work.String(),
		Endpoint: endpoint.NewEndpoint("grpc", server.ListenAddr(), false).String(),
	})

	meta := cluster.RequestMetadata{
		Deadline:      time.Now().Add(2 * time.Second),
		CorrelationID: "corr-linker-123",
	}
	if err := linker.Deliver(context.Background(), &DeliverArgs{
		NID:      "node-metadata",
		CID:      11,
		UID:      22,
		Token:    session.Token{UID: 22, Generation: 7},
		Metadata: meta,
		Buffer:   []byte("hello"),
	}); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-provider.got:
		if got.CorrelationID != meta.CorrelationID {
			t.Fatalf("correlation_id=%q, want %q", got.CorrelationID, meta.CorrelationID)
		}
		if got.Deadline.UnixMilli() != meta.Deadline.UnixMilli() {
			t.Fatalf("deadline=%v, want %v", got.Deadline, meta.Deadline)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("linker metadata did not reach node provider")
	}
}
