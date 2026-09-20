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

func TestBuilder(t *testing.T) {
	serverAddr := startNodeTestServer(t)
	builder := node.NewBuilder(&node.ClientOptions{
		ID:                xuuid.UUID(),
		Kind:              cluster.Gate,
		ConnNum:           10,
		DialTimeout:       3 * time.Second,
		DialRetryTimes:    3,
		WriteTimeout:      1 * time.Second,
		WriteQueueSize:    1024,
		CallTimeout:       3 * time.Second,
		FaultRecoveryTime: 3 * time.Second,
	})

	client, err := builder.Build(serverAddr)
	if err != nil {
		t.Fatal(err)
	}

	err = client.Deliver(context.Background(), 1, 2, session.Token{UID: 2, Generation: 1}, buffer.NewNocopyBuffer([]byte("hello world")))
	if err != nil {
		t.Fatal(err)
	}
}

func startNodeTestServer(t *testing.T) string {
	t.Helper()
	server, err := node.NewServer(&provider{}, &node.ServerOptions{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Start()
	}()

	deadline := time.Now().Add(3 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", server.ListenAddr(), 50*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			break
		}

		select {
		case startErr := <-errCh:
			if startErr != nil {
				t.Fatalf("start node test server: %v", startErr)
			}
			t.Fatal("node test server stopped before becoming ready")
		default:
		}

		if time.Now().After(deadline) {
			t.Fatalf("node test server did not listen on %s", server.ListenAddr())
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Cleanup(func() {
		if err := server.Stop(); err != nil {
			t.Errorf("stop node test server: %v", err)
		}
		if err := <-errCh; err != nil {
			t.Errorf("node test server exited with error: %v", err)
		}
	})
	return server.ListenAddr()
}
