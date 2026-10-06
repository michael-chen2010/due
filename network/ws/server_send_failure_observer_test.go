package ws

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

func TestServerSendFailureObserverReportsQueueByteBudgetReject(t *testing.T) {
	var (
		mu      sync.Mutex
		reasons []string
	)
	server := NewServer(
		WithServerMaxWriteQueueBytes(1),
		WithServerSendFailureObserver(func(reason string) {
			mu.Lock()
			reasons = append(reasons, reason)
			mu.Unlock()
		}),
	).(*server)
	conn := newWriteBudgetTestConn(server)

	if err := conn.doWriteToQueue(conn.lowPriorityQueue, dataPacket, []byte("12")); !errors.Is(err, ErrWriteQueueBytesExceeded) {
		t.Fatalf("doWriteToQueue() error = %v, want ErrWriteQueueBytesExceeded", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(reasons) != 1 || reasons[0] != SendFailureQueueBytesExceeded {
		t.Fatalf("reasons = %v, want [%s]", reasons, SendFailureQueueBytesExceeded)
	}
}

func TestServerSendFailureObserverReportsEnqueueTimeout(t *testing.T) {
	var reasons []string
	server := NewServer(
		WithServerWriteTimeout(5*time.Millisecond),
		WithServerSendFailureObserver(func(reason string) {
			reasons = append(reasons, reason)
		}),
	).(*server)
	conn := &serverConn{
		connMgr:           server.connMgr,
		lowPriorityQueue:  make(chan *task),
		highPriorityQueue: make(chan *task),
		taskPool: sync.Pool{New: func() any {
			return &task{}
		}},
	}

	err := conn.doWriteToQueue(conn.lowPriorityQueue, dataPacket, []byte("x"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("doWriteToQueue() error = %v, want context deadline exceeded", err)
	}
	if len(reasons) != 1 || reasons[0] != SendFailureEnqueueTimeout {
		t.Fatalf("reasons = %v, want [%s]", reasons, SendFailureEnqueueTimeout)
	}
}

func TestServerSendFailureObserverReportsSocketWriteTimeout(t *testing.T) {
	var reasons []string
	server := NewServer(
		WithServerSocketWriteTimeout(time.Second),
		WithServerSendFailureObserver(func(reason string) {
			reasons = append(reasons, reason)
		}),
	).(*server)
	conn := &serverConn{connMgr: server.connMgr}

	err := conn.writeMessage(timeoutMessageWriter{}, []byte("x"))
	if err == nil {
		t.Fatal("writeMessage() error = nil, want timeout")
	}
	if len(reasons) != 1 || reasons[0] != SendFailureSocketWriteTimeout {
		t.Fatalf("reasons = %v, want [%s]", reasons, SendFailureSocketWriteTimeout)
	}
}

func TestServerSendFailureObserverReportsSocketWriteError(t *testing.T) {
	var reasons []string
	server := NewServer(
		WithServerSendFailureObserver(func(reason string) {
			reasons = append(reasons, reason)
		}),
	).(*server)
	conn := &serverConn{connMgr: server.connMgr}

	err := conn.writeMessage(errorMessageWriter{}, []byte("x"))
	if err == nil {
		t.Fatal("writeMessage() error = nil, want generic write error")
	}
	if len(reasons) != 1 || reasons[0] != SendFailureSocketWriteError {
		t.Fatalf("reasons = %v, want [%s]", reasons, SendFailureSocketWriteError)
	}
}

func TestServerSendFailureObserverPanicDoesNotChangeSendError(t *testing.T) {
	server := NewServer(
		WithServerMaxWriteQueueBytes(1),
		WithServerSendFailureObserver(func(string) {
			panic("observer failure")
		}),
	).(*server)
	conn := newWriteBudgetTestConn(server)

	if err := conn.doWriteToQueue(conn.lowPriorityQueue, dataPacket, []byte("12")); !errors.Is(err, ErrWriteQueueBytesExceeded) {
		t.Fatalf("doWriteToQueue() error = %v, want ErrWriteQueueBytesExceeded", err)
	}
}

type errorMessageWriter struct{}

func (errorMessageWriter) SetWriteDeadline(time.Time) error { return nil }

func (errorMessageWriter) WriteMessage(int, []byte) error {
	return errors.New("write failed")
}

type timeoutMessageWriter struct{}

func (timeoutMessageWriter) SetWriteDeadline(time.Time) error { return nil }

func (timeoutMessageWriter) WriteMessage(int, []byte) error {
	return timeoutNetError{}
}

type timeoutNetError struct{}

func (timeoutNetError) Error() string   { return "write timeout" }
func (timeoutNetError) Timeout() bool   { return true }
func (timeoutNetError) Temporary() bool { return true }

var _ net.Error = timeoutNetError{}
