package ws

import (
	"errors"
	"sync"
	"testing"
)

func newWriteBudgetTestConn(server *server) *serverConn {
	return &serverConn{
		connMgr:           server.connMgr,
		lowPriorityQueue:  make(chan *task, 8),
		highPriorityQueue: make(chan *task, 8),
		taskPool: sync.Pool{New: func() any {
			return &task{}
		}},
	}
}

func TestServerWriteQueueByteBudgetIsSharedAndReservesHighPriorityCapacity(t *testing.T) {
	server := NewServer(
		WithServerMaxWriteQueueBytes(10),
		WithServerHighPriorityReserveBytes(4),
	).(*server)
	first := newWriteBudgetTestConn(server)
	second := newWriteBudgetTestConn(server)

	if err := first.doWriteToQueue(first.lowPriorityQueue, dataPacket, []byte("123456")); err != nil {
		t.Fatalf("enqueue low 6 bytes: %v", err)
	}
	if err := second.doWriteToQueue(second.lowPriorityQueue, dataPacket, []byte("x")); !errors.Is(err, ErrWriteQueueBytesExceeded) {
		t.Fatalf("enqueue low above non-reserved budget error = %v, want ErrWriteQueueBytesExceeded", err)
	}

	if err := second.doWriteToQueue(second.highPriorityQueue, dataPacket, []byte("1234")); err != nil {
		t.Fatalf("enqueue reserved high 4 bytes: %v", err)
	}
	if err := first.doWriteToQueue(first.highPriorityQueue, dataPacket, []byte("x")); !errors.Is(err, ErrWriteQueueBytesExceeded) {
		t.Fatalf("enqueue high above total budget error = %v, want ErrWriteQueueBytesExceeded", err)
	}
}

func TestServerWriteQueueByteBudgetReturnsOnlyAfterQueuedTasksAreRecycled(t *testing.T) {
	server := NewServer(
		WithServerMaxWriteQueueBytes(10),
		WithServerHighPriorityReserveBytes(4),
	).(*server)
	first := newWriteBudgetTestConn(server)
	second := newWriteBudgetTestConn(server)

	if err := first.doWriteToQueue(first.lowPriorityQueue, dataPacket, []byte("123456")); err != nil {
		t.Fatalf("enqueue first low: %v", err)
	}
	if err := first.doWriteToQueue(first.highPriorityQueue, dataPacket, []byte("1234")); err != nil {
		t.Fatalf("enqueue first high: %v", err)
	}
	if err := second.doWriteToQueue(second.highPriorityQueue, dataPacket, []byte("x")); !errors.Is(err, ErrWriteQueueBytesExceeded) {
		t.Fatalf("budget returned before task recycle, error = %v", err)
	}

	first.discardQueuedTasks()

	if err := second.doWriteToQueue(second.lowPriorityQueue, dataPacket, []byte("123456")); err != nil {
		t.Fatalf("enqueue low after recycle: %v", err)
	}
	if err := second.doWriteToQueue(second.highPriorityQueue, dataPacket, []byte("1234")); err != nil {
		t.Fatalf("enqueue high after recycle: %v", err)
	}
}
