package ws

import (
	"errors"
	"net"
	"testing"
)

func TestServerListenBacklogDefaultsDisabledAndCanOverride(t *testing.T) {
	defaultServer := NewServer().(*server)
	if defaultServer.opts.listenBacklog != 0 {
		t.Fatalf("default listen backlog = %d, want 0", defaultServer.opts.listenBacklog)
	}

	configuredServer := NewServer(WithServerListenBacklog(2048)).(*server)
	if configuredServer.opts.listenBacklog != 2048 {
		t.Fatalf("configured listen backlog = %d, want 2048", configuredServer.opts.listenBacklog)
	}
}

func TestServerInitAppliesConfiguredListenBacklog(t *testing.T) {
	original := configureServerListenBacklog
	defer func() { configureServerListenBacklog = original }()

	var gotBacklog int
	configureServerListenBacklog = func(_ *net.TCPListener, backlog int) error {
		gotBacklog = backlog
		return nil
	}

	server := NewServer(
		WithServerAddr("127.0.0.1:0"),
		WithServerListenBacklog(2048),
	).(*server)
	if err := server.init(); err != nil {
		t.Fatalf("init: %v", err)
	}
	defer server.listener.Close()

	if gotBacklog != 2048 {
		t.Fatalf("applied listen backlog = %d, want 2048", gotBacklog)
	}
}

func TestServerInitReturnsBacklogApplyError(t *testing.T) {
	original := configureServerListenBacklog
	defer func() { configureServerListenBacklog = original }()

	wantErr := errors.New("listen backlog failed")
	configureServerListenBacklog = func(_ *net.TCPListener, _ int) error {
		return wantErr
	}

	server := NewServer(
		WithServerAddr("127.0.0.1:0"),
		WithServerListenBacklog(2048),
	).(*server)
	if err := server.init(); !errors.Is(err, wantErr) {
		t.Fatalf("init error = %v, want %v", err, wantErr)
	}
}
