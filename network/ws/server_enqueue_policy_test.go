package ws

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dobyte/due/v2/network"
)

func TestServerHighPriorityEnqueueTimeoutMarksSlowConnectionClosed(t *testing.T) {
	server := NewServer(
		WithServerWriteTimeout(10 * time.Millisecond),
	).(*server)
	conn := newWriteBudgetTestConn(server)
	conn.state.Store(int32(network.ConnOpened))
	conn.highPriorityQueue = make(chan *task, 1)
	conn.highPriorityQueue <- &task{typ: dataPacket, msg: []byte("already queued")}

	err := conn.doWriteToQueue(conn.highPriorityQueue, dataPacket, []byte("critical-response"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("high-priority enqueue error = %v, want context deadline exceeded", err)
	}
	if got := conn.handleHighPriorityEnqueueResult(err); !errors.Is(got, context.DeadlineExceeded) {
		t.Fatalf("handleHighPriorityEnqueueResult() = %v, want original deadline error", got)
	}
	if got := conn.State(); got != network.ConnClosed {
		t.Fatalf("connection state = %v, want ConnClosed after high-priority enqueue timeout", got)
	}
}

func TestServerLowPriorityEnqueueTimeoutDoesNotCloseConnection(t *testing.T) {
	server := NewServer(
		WithServerWriteTimeout(10 * time.Millisecond),
	).(*server)
	conn := newWriteBudgetTestConn(server)
	conn.state.Store(int32(network.ConnOpened))
	conn.lowPriorityQueue = make(chan *task, 1)
	conn.lowPriorityQueue <- &task{typ: dataPacket, msg: []byte("already queued")}

	err := conn.doWriteToQueue(conn.lowPriorityQueue, dataPacket, []byte("best-effort-push"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("low-priority enqueue error = %v, want context deadline exceeded", err)
	}
	if got := conn.State(); got != network.ConnOpened {
		t.Fatalf("connection state = %v, want ConnOpened for low-priority enqueue timeout", got)
	}
}
