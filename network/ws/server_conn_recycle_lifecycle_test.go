package ws

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

const websocketAcceptGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

type blockingWebSocketNetConn struct {
	net.Conn

	blockWrites atomic.Bool
	started     chan struct{}
	release     chan struct{}
	returned    chan struct{}
	startOnce   sync.Once
	releaseOnce sync.Once
	returnOnce  sync.Once
}

func newBlockingWebSocketNetConn(conn net.Conn) *blockingWebSocketNetConn {
	return &blockingWebSocketNetConn{
		Conn:     conn,
		started:  make(chan struct{}),
		release:  make(chan struct{}),
		returned: make(chan struct{}),
	}
}

func (c *blockingWebSocketNetConn) Write(p []byte) (int, error) {
	if !c.blockWrites.Load() {
		return c.Conn.Write(p)
	}

	c.startOnce.Do(func() { close(c.started) })
	<-c.release
	c.returnOnce.Do(func() { close(c.returned) })
	return 0, net.ErrClosed
}

func (c *blockingWebSocketNetConn) releaseWrite() {
	c.releaseOnce.Do(func() { close(c.release) })
}

func newPipeWebSocket(t *testing.T) (*websocket.Conn, *blockingWebSocketNetConn, net.Conn) {
	t.Helper()

	clientNet, serverNet := net.Pipe()
	blockingConn := newBlockingWebSocketNetConn(clientNet)
	handshakeErr := make(chan error, 1)

	go func() {
		reader := bufio.NewReader(serverNet)
		req, err := http.ReadRequest(reader)
		if err != nil {
			handshakeErr <- err
			return
		}
		key := req.Header.Get("Sec-WebSocket-Key")
		sum := sha1.Sum([]byte(key + websocketAcceptGUID))
		accept := base64.StdEncoding.EncodeToString(sum[:])
		_, err = fmt.Fprintf(
			serverNet,
			"HTTP/1.1 101 Switching Protocols\r\n"+
				"Upgrade: websocket\r\n"+
				"Connection: Upgrade\r\n"+
				"Sec-WebSocket-Accept: %s\r\n\r\n",
			accept,
		)
		handshakeErr <- err
	}()

	u, err := url.Parse("ws://pipe.local/")
	if err != nil {
		t.Fatalf("url.Parse: %v", err)
	}
	wsConn, _, err := websocket.NewClient(blockingConn, u, http.Header{}, 1024, 1024)
	if err != nil {
		_ = serverNet.Close()
		t.Fatalf("websocket.NewClient: %v", err)
	}
	if err := <-handshakeErr; err != nil {
		_ = wsConn.Close()
		_ = serverNet.Close()
		t.Fatalf("server handshake: %v", err)
	}

	return wsConn, blockingConn, serverNet
}

func TestServerConnRecycleWaitsForWriterExitBeforeReset(t *testing.T) {
	server := NewServer(
		WithServerWriteQueueSize(8),
		WithServerHeartbeatInterval(0),
	).(*server)
	wsConn, blockingConn, serverNet := newPipeWebSocket(t)
	defer func() {
		blockingConn.releaseWrite()
		select {
		case <-blockingConn.returned:
		case <-time.After(time.Second):
		}
		_ = wsConn.Close()
		_ = serverNet.Close()
	}()

	if err := server.connMgr.allocate(wsConn); err != nil {
		t.Fatalf("allocate: %v", err)
	}

	index := int(reflect.ValueOf(wsConn).Pointer()) % len(server.connMgr.partitions)
	partition := server.connMgr.partitions[index]
	partition.rw.RLock()
	conn := partition.connections[wsConn]
	partition.rw.RUnlock()
	if conn == nil {
		t.Fatal("allocated server connection not registered")
	}
	if conn.attr == nil {
		t.Fatal("allocated server connection attr is nil")
	}

	blockingConn.blockWrites.Store(true)
	if err := conn.Send([]byte("hold writer in old connection lifetime")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	select {
	case <-blockingConn.started:
	case <-time.After(time.Second):
		t.Fatal("writer did not enter blocked socket write")
	}

	if err := conn.forceClose(true); err != nil {
		t.Fatalf("forceClose: %v", err)
	}

	if conn.attr == nil {
		t.Fatal("server connection reset/recycled before blocked writer exited")
	}

	blockingConn.releaseWrite()
	select {
	case <-blockingConn.returned:
	case <-time.After(time.Second):
		t.Fatal("blocked writer did not return after release")
	}
}
