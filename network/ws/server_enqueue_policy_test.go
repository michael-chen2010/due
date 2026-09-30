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

func TestServerLowPriorityEnqueueTimeoutClosesOnlyAfterConfiguredRepeatLimit(t *testing.T) {
	server := NewServer(
		WithServerWriteTimeout(10*time.Millisecond),
		WithServerSlowConsumerEnqueueTimeoutLimit(3),
	).(*server)
	conn := newWriteBudgetTestConn(server)
	conn.state.Store(int32(network.ConnOpened))
	conn.lowPriorityQueue = make(chan *task, 1)
	conn.lowPriorityQueue <- &task{typ: dataPacket, msg: []byte("already queued")}

	for attempt := 1; attempt <= 3; attempt++ {
		err := conn.doWriteToQueue(conn.lowPriorityQueue, dataPacket, []byte("best-effort-push"))
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("attempt %d low-priority enqueue error = %v, want context deadline exceeded", attempt, err)
		}
		if got := conn.handleLowPriorityEnqueueResult(err); !errors.Is(got, context.DeadlineExceeded) {
			t.Fatalf("attempt %d handleLowPriorityEnqueueResult() = %v, want original deadline error", attempt, got)
		}
		if attempt < 3 {
			if got := conn.State(); got != network.ConnOpened {
				t.Fatalf("attempt %d connection state = %v, want ConnOpened before repeat limit", attempt, got)
			}
		}
	}
	if got := conn.State(); got != network.ConnClosed {
		t.Fatalf("connection state = %v, want ConnClosed after repeated low-priority enqueue timeouts", got)
	}
}

func TestServerLowPrioritySuccessfulEnqueueResetsTimeoutStreak(t *testing.T) {
	server := NewServer(
		WithServerWriteTimeout(10*time.Millisecond),
		WithServerSlowConsumerEnqueueTimeoutLimit(3),
	).(*server)
	conn := newWriteBudgetTestConn(server)
	conn.state.Store(int32(network.ConnOpened))

	if got := conn.handleLowPriorityEnqueueResult(context.DeadlineExceeded); !errors.Is(got, context.DeadlineExceeded) {
		t.Fatalf("first timeout result = %v", got)
	}
	if got := conn.handleLowPriorityEnqueueResult(context.DeadlineExceeded); !errors.Is(got, context.DeadlineExceeded) {
		t.Fatalf("second timeout result = %v", got)
	}
	if err := conn.handleLowPriorityEnqueueResult(nil); err != nil {
		t.Fatalf("successful enqueue result = %v", err)
	}
	for attempt := 1; attempt <= 2; attempt++ {
		_ = conn.handleLowPriorityEnqueueResult(context.DeadlineExceeded)
	}
	if got := conn.State(); got != network.ConnOpened {
		t.Fatalf("connection state = %v, want ConnOpened because success reset timeout streak", got)
	}
	_ = conn.handleLowPriorityEnqueueResult(context.DeadlineExceeded)
	if got := conn.State(); got != network.ConnClosed {
		t.Fatalf("connection state = %v, want ConnClosed after a new streak reaches limit", got)
	}
}
