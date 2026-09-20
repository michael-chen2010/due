package gate_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/dobyte/due/v2/cluster"
	"github.com/dobyte/due/v2/internal/transporter/gate"
	"github.com/dobyte/due/v2/session"
	"github.com/dobyte/due/v2/utils/xuuid"
)

func TestBuilder(t *testing.T) {
	serverAddr := startGateTestServer(t)
	builder := gate.NewBuilder(&gate.ClientOptions{
		ID:                xuuid.UUID(),
		Kind:              cluster.Node,
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

	ip, err := client.GetIP(context.Background(), session.User, 1)
	if err != nil {
		t.Fatal(err)
	}

	if ip != "192.168.0.88" {
		t.Fatalf("GetIP() = %q, want %q", ip, "192.168.0.88")
	}

	ip, err = client.GetIP(context.Background(), session.User, 1)
	if err != nil {
		t.Fatal(err)
	}

	if ip != "192.168.0.88" {
		t.Fatalf("second GetIP() = %q, want %q", ip, "192.168.0.88")
	}
}

func startGateTestServer(t *testing.T) string {
	t.Helper()
	server, err := gate.NewServer(&provider{}, &gate.ServerOptions{Addr: "127.0.0.1:0"})
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
				t.Fatalf("start gate test server: %v", startErr)
			}
			t.Fatal("gate test server stopped before becoming ready")
		default:
		}

		if time.Now().After(deadline) {
			t.Fatalf("gate test server did not listen on %s", server.ListenAddr())
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Cleanup(func() {
		if err := server.Stop(); err != nil {
			t.Errorf("stop gate test server: %v", err)
		}
		if err := <-errCh; err != nil {
			t.Errorf("gate test server exited with error: %v", err)
		}
	})
	return server.ListenAddr()
}

func TestBuilder_Fault(t *testing.T) {
	builder := gate.NewBuilder(&gate.ClientOptions{
		ID:                xuuid.UUID(),
		Kind:              cluster.Node,
		ConnNum:           10,
		DialTimeout:       3 * time.Second,
		DialRetryTimes:    3,
		WriteTimeout:      1 * time.Second,
		WriteQueueSize:    1024,
		CallTimeout:       3 * time.Second,
		FaultRecoveryTime: 3 * time.Second,
	})

	for i := range 3 {
		if _, err := builder.Build("127.0.0.1:49899"); err != nil {
			t.Log(err)
			time.Sleep(time.Duration(i+1) * time.Second)
		} else {
			t.Log("build success")
		}
	}
}
