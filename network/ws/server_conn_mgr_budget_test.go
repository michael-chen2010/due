package ws

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dobyte/due/v2/network"
	"github.com/gorilla/websocket"
)

type serverConnMgrWebSocketPair struct {
	server *websocket.Conn
	client *websocket.Conn
	http   *httptest.Server
}

func newServerConnMgrWebSocketPair(t *testing.T) *serverConnMgrWebSocketPair {
	t.Helper()

	accepted := make(chan *websocket.Conn, 1)
	upgradeErr := make(chan error, 1)
	upgrader := websocket.Upgrader{
		CheckOrigin: func(*http.Request) bool { return true },
	}
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			upgradeErr <- err
			return
		}
		accepted <- conn
	}))

	endpoint := "ws" + strings.TrimPrefix(httpServer.URL, "http")
	clientConn, _, err := websocket.DefaultDialer.Dial(endpoint, nil)
	if err != nil {
		httpServer.Close()
		t.Fatalf("dial websocket pair: %v", err)
	}

	var serverConn *websocket.Conn
	select {
	case err := <-upgradeErr:
		_ = clientConn.Close()
		httpServer.Close()
		t.Fatalf("upgrade websocket pair: %v", err)
	case serverConn = <-accepted:
	case <-time.After(time.Second):
		_ = clientConn.Close()
		httpServer.Close()
		t.Fatal("timed out waiting for websocket pair")
	}

	return &serverConnMgrWebSocketPair{
		server: serverConn,
		client: clientConn,
		http:   httpServer,
	}
}

func (p *serverConnMgrWebSocketPair) close() {
	if p == nil {
		return
	}
	if p.client != nil {
		_ = p.client.Close()
	}
	if p.server != nil {
		_ = p.server.Close()
	}
	if p.http != nil {
		p.http.Close()
	}
}

func TestServerConnMgrImmediateConnectCloseReleasesReservedSlot(t *testing.T) {
	server := NewServer(WithServerMaxConnNum(1)).(*server)
	closed := make(chan struct{}, 1)
	server.OnConnect(func(conn network.Conn) {
		if err := conn.Close(true); err != nil {
			t.Errorf("close connection from OnConnect: %v", err)
		}
		closed <- struct{}{}
	})

	pair := newServerConnMgrWebSocketPair(t)
	defer pair.close()

	if err := server.connMgr.allocate(pair.server); err != nil {
		t.Fatalf("allocate first connection: %v", err)
	}

	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("OnConnect did not close the connection")
	}

	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		if server.connMgr.total.Load() == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf(
		"connection total=%d after synchronous OnConnect close, want 0",
		server.connMgr.total.Load(),
	)
}

func TestServerConnMgrConcurrentAdmissionNeverExceedsMaxConnections(t *testing.T) {
	const connectionCount = 4

	server := NewServer(WithServerMaxConnNum(1)).(*server)
	entered := make(chan struct{}, connectionCount)
	release := make(chan struct{})
	server.OnConnect(func(network.Conn) {
		entered <- struct{}{}
		<-release
	})

	pairs := make([]*serverConnMgrWebSocketPair, 0, connectionCount)
	for range connectionCount {
		pair := newServerConnMgrWebSocketPair(t)
		pairs = append(pairs, pair)
		defer pair.close()
	}

	start := make(chan struct{})
	errs := make(chan error, connectionCount)
	var wg sync.WaitGroup
	wg.Add(connectionCount)
	for _, pair := range pairs {
		go func(pair *serverConnMgrWebSocketPair) {
			defer wg.Done()
			<-start
			err := server.connMgr.allocate(pair.server)
			if err != nil {
				_ = pair.server.Close()
			}
			errs <- err
		}(pair)
	}
	close(start)

	select {
	case <-entered:
	case <-time.After(time.Second):
		close(release)
		wg.Wait()
		t.Fatal("no connection reached OnConnect")
	}

	secondEntered := false
	select {
	case <-entered:
		secondEntered = true
	case <-time.After(150 * time.Millisecond):
	}

	close(release)
	wg.Wait()
	close(errs)

	if secondEntered {
		t.Fatal("more than one concurrent connection entered OnConnect with max connections = 1")
	}

	successes := 0
	for err := range errs {
		if err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("successful allocations=%d, want exactly 1", successes)
	}
}
