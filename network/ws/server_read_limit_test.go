package ws

import (
	"bytes"
	"net/http"
	"testing"
	"time"

	"github.com/dobyte/due/v2/network"
	"github.com/dobyte/due/v2/packet"
	"github.com/gorilla/websocket"
)

func readLimitServer(t *testing.T, options ...ServerOption) (string, <-chan []byte) {
	t.Helper()
	options = append([]ServerOption{WithServerAddr("127.0.0.1:0"), WithServerHeartbeatInterval(0),
		WithServerPacketHeartbeatInspection(false), WithServerSocketWriteTimeout(time.Second)}, options...)
	s := NewServer(options...).(*server)
	received := make(chan []byte, 4)
	s.OnReceive(func(_ network.Conn, b []byte) { received <- append([]byte(nil), b...) })
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Stop(); err != nil {
			t.Error(err)
		}
	})
	return "ws://" + s.listener.Addr().String() + "/", received
}

func readLimitClient(t *testing.T, addr string) *websocket.Conn {
	t.Helper()
	dialer := websocket.Dialer{WriteBufferSize: 32, HandshakeTimeout: time.Second}
	c, _, err := dialer.Dial(addr, http.Header{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func requireMessageTooBig(t *testing.T, c *websocket.Conn, received <-chan []byte) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err := c.ReadMessage()
	if !websocket.IsCloseError(err, websocket.CloseMessageTooBig) {
		t.Fatalf("oversized message must close with 1009 before business dispatch: %v", err)
	}
	select {
	case b := <-received:
		t.Fatalf("oversized message reached business handler: %d bytes", len(b))
	default:
	}
}

func TestServerReadLimitRejectsBeforeReadingAdvertisedPayload(t *testing.T) {
	addr, received := readLimitServer(t, WithServerMaxReadMessageBytes(64))
	c := readLimitClient(t, addr)
	// Valid masked binary frame header advertising 4096 bytes, with no body.
	// A business-layer len check cannot reject until that entire body arrives.
	if _, err := c.UnderlyingConn().Write([]byte{0x82, 0xfe, 0x10, 0, 0, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	requireMessageTooBig(t, c, received)
}

func TestServerReadLimitAppliesToWholeFragmentedMessage(t *testing.T) {
	addr, received := readLimitServer(t, WithServerMaxReadMessageBytes(64))
	c := readLimitClient(t, addr)
	w, err := c.NextWriter(websocket.BinaryMessage)
	if err != nil {
		t.Fatal(err)
	}
	// Force many small continuation frames whose total crosses the limit.
	_, _ = w.Write(bytes.Repeat([]byte("f"), 256))
	_ = w.Close()
	requireMessageTooBig(t, c, received)
}

func TestServerReadLimitIncludesTextMessages(t *testing.T) {
	addr, received := readLimitServer(t, WithServerMaxReadMessageBytes(64))
	c := readLimitClient(t, addr)
	_ = c.WriteMessage(websocket.TextMessage, bytes.Repeat([]byte("t"), 65))
	requireMessageTooBig(t, c, received)
}

func TestServerReadLimitResetsForEachMessageAndAcceptsExactBoundary(t *testing.T) {
	addr, received := readLimitServer(t, WithServerMaxReadMessageBytes(64))
	c := readLimitClient(t, addr)
	for i := 0; i < 2; i++ {
		payload := bytes.Repeat([]byte{byte(i + 1)}, 64)
		if err := c.WriteMessage(websocket.BinaryMessage, payload); err != nil {
			t.Fatal(err)
		}
		select {
		case b := <-received:
			if !bytes.Equal(b, payload) {
				t.Fatal("boundary message changed")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("boundary message rejected")
		}
	}
}

func TestServerReadLimitDefaultPreservesLegacyMessages(t *testing.T) {
	addr, received := readLimitServer(t)
	c := readLimitClient(t, addr)
	payload := bytes.Repeat([]byte("legacy"), 2048)
	if err := c.WriteMessage(websocket.BinaryMessage, payload); err != nil {
		t.Fatal(err)
	}
	select {
	case b := <-received:
		if !bytes.Equal(b, payload) {
			t.Fatal("default changed legacy payload")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("default rejected previously valid legacy message")
	}
}

func TestServerReadLimitDefaultPreservesLegacyDuePacket(t *testing.T) {
	addr, received := readLimitServer(t, WithServerPacketHeartbeatInspection(true))
	c := readLimitClient(t, addr)
	// Stay within the unchanged legacy Due packet buffer budget (5000 bytes).
	payload, err := packet.PackMessage(&packet.Message{Seq: 7, Route: 1000, Buffer: bytes.Repeat([]byte("legacy"), 512)})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.WriteMessage(websocket.BinaryMessage, payload); err != nil {
		t.Fatal(err)
	}
	select {
	case b := <-received:
		if !bytes.Equal(b, payload) {
			t.Fatal("default changed legacy Due packet")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("default rejected legacy Due packet with default heartbeat inspection")
	}
}
