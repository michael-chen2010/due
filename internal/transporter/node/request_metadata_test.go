package node_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/dobyte/due/v2/cluster"
	"github.com/dobyte/due/v2/core/buffer"
	"github.com/dobyte/due/v2/internal/transporter/node"
	"github.com/dobyte/due/v2/session"
	"github.com/dobyte/due/v2/utils/xuuid"
)

type requestMetadataProvider struct {
	got chan cluster.RequestMetadata
}

func (p *requestMetadataProvider) Trigger(context.Context, string, int64, int64, session.Token, cluster.Event) error {
	return nil
}

func (p *requestMetadataProvider) Deliver(ctx context.Context, _, _ string, _, _ int64, _ session.Token, _ []byte) error {
	meta, ok := cluster.RequestMetadataFromContext(ctx)
	if !ok {
		p.got <- cluster.RequestMetadata{}
		return nil
	}
	p.got <- meta
	return nil
}

func (p *requestMetadataProvider) GetState() (cluster.State, error) {
	return cluster.Work, nil
}

func (p *requestMetadataProvider) SetState(cluster.State) error {
	return nil
}

func TestDeliverCarriesRequestMetadataAcrossTransport(t *testing.T) {
	provider := &requestMetadataProvider{got: make(chan cluster.RequestMetadata, 1)}
	server, err := node.NewServer(provider, &node.ServerOptions{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- server.Start() }()
	t.Cleanup(func() {
		if err := server.Stop(); err != nil {
			t.Errorf("stop node metadata server: %v", err)
		}
		if err := <-errCh; err != nil {
			t.Errorf("node metadata server exited: %v", err)
		}
	})

	deadline := time.Now().Add(3 * time.Second)
	listenDeadline := time.Now().Add(3 * time.Second)
	for {
		conn, dialErr := net.DialTimeout("tcp", server.ListenAddr(), 50*time.Millisecond)
		if dialErr == nil {
			_ = conn.Close()
			break
		}
		if time.Now().After(listenDeadline) {
			t.Fatalf("node metadata server did not listen: %v", dialErr)
		}
		time.Sleep(10 * time.Millisecond)
	}

	builder := node.NewBuilder(&node.ClientOptions{
		ID:                xuuid.UUID(),
		Kind:              cluster.Gate,
		ConnNum:           1,
		DialTimeout:       time.Second,
		DialRetryTimes:    1,
		WriteTimeout:      time.Second,
		WriteQueueSize:    64,
		CallTimeout:       time.Second,
		FaultRecoveryTime: time.Second,
	})
	client, err := builder.Build(server.ListenAddr())
	if err != nil {
		t.Fatal(err)
	}

	meta := cluster.RequestMetadata{
		Deadline:      deadline,
		CorrelationID: "corr-transport-123",
	}
	err = client.DeliverWithMetadata(
		context.Background(),
		11,
		22,
		session.Token{UID: 22, Generation: 7},
		meta,
		buffer.NewNocopyBuffer([]byte("hello")),
	)
	if err != nil {
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
		t.Fatal("provider did not receive request metadata")
	}
}
