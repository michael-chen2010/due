package ws

import (
	"errors"
	"testing"
	"time"

	"github.com/dobyte/due/v2/network"
)

type fakeWebSocketMessageWriter struct {
	deadline time.Time
	writeErr error
}

func (w *fakeWebSocketMessageWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadline = deadline
	return nil
}

func (w *fakeWebSocketMessageWriter) WriteMessage(_ int, _ []byte) error {
	return w.writeErr
}

func TestServerHandleQueuedWriteClosesConnectionAfterWriteFailure(t *testing.T) {
	server := NewServer(
		WithServerSocketWriteTimeout(25 * time.Millisecond),
	).(*server)
	conn := newWriteBudgetTestConn(server)
	conn.state.Store(int32(network.ConnOpened))
	writer := &fakeWebSocketMessageWriter{writeErr: errors.New("slow consumer write timeout")}
	task := &task{
		typ: dataPacket,
		msg: []byte("critical-response"),
	}

	if ok := conn.handleQueuedWrite(writer, task); ok {
		t.Fatal("handleQueuedWrite() continued after socket write failure")
	}
	if got := conn.State(); got != network.ConnClosed {
		t.Fatalf("connection state = %v, want ConnClosed", got)
	}
}

func TestServerDoWriteAppliesSocketDeadlineAndStopsOnWriteFailure(t *testing.T) {
	server := NewServer(
		WithServerSocketWriteTimeout(25 * time.Millisecond),
	).(*server)
	conn := newWriteBudgetTestConn(server)
	writer := &fakeWebSocketMessageWriter{writeErr: errors.New("slow consumer write timeout")}
	task := &task{
		typ:         dataPacket,
		msg:         []byte("critical-response"),
		queuedBytes: int64(len("critical-response")),
	}

	started := time.Now()
	if ok := conn.doWrite(writer, task); ok {
		t.Fatal("doWrite() continued after socket write failure")
	}
	if writer.deadline.IsZero() {
		t.Fatal("socket write deadline was not configured")
	}
	if writer.deadline.Before(started.Add(20*time.Millisecond)) ||
		writer.deadline.After(started.Add(250*time.Millisecond)) {
		t.Fatalf("socket write deadline = %v, want approximately started+25ms", writer.deadline)
	}
}
