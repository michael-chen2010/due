package client

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/dobyte/due/v2/core/buffer"
	"github.com/dobyte/due/v2/internal/transporter/internal/def"
)

type callLifecycleConn struct{}

func (callLifecycleConn) Read([]byte) (int, error)         { return 0, nil }
func (callLifecycleConn) Write(p []byte) (int, error)      { return len(p), nil }
func (callLifecycleConn) Close() error                     { return nil }
func (callLifecycleConn) LocalAddr() net.Addr              { return nil }
func (callLifecycleConn) RemoteAddr() net.Addr             { return nil }
func (callLifecycleConn) SetDeadline(time.Time) error      { return nil }
func (callLifecycleConn) SetReadDeadline(time.Time) error  { return nil }
func (callLifecycleConn) SetWriteDeadline(time.Time) error { return nil }

func TestCallTimeoutRemovesOriginalPendingSequenceAfterWrite(t *testing.T) {
	const seq uint64 = 77

	cli := NewClient("", &Options{
		CallTimeout:    20 * time.Millisecond,
		WriteQueueSize: 8,
	})
	c := newConn(cli)
	c.state.Store(def.ConnOpened)
	cli.conns = []*conn{c}

	result := make(chan error, 1)
	go func() {
		_, err := cli.Call(
			context.Background(),
			seq,
			buffer.NewNocopyBuffer([]byte("request")),
		)
		result <- err
	}()

	var msg *message
	select {
	case msg = <-c.queue:
	case <-time.After(time.Second):
		t.Fatal("Call did not enqueue request")
	}

	if ok := c.doWrite(callLifecycleConn{}, msg); !ok {
		t.Fatal("doWrite failed")
	}

	select {
	case err := <-result:
		if err == nil {
			t.Fatal("Call timeout error=nil")
		}
	case <-time.After(time.Second):
		t.Fatal("Call did not return after timeout")
	}

	if _, ok := c.pending.extract(seq); ok {
		t.Fatalf("pending seq=%d remained after Call timeout", seq)
	}
}

func TestCallResponsesStayBoundToOriginalCallAfterEnvelopeReuse(t *testing.T) {
	cli := NewClient("", &Options{
		CallTimeout:    time.Second,
		WriteQueueSize: 8,
	})
	c := newConn(cli)
	c.state.Store(def.ConnOpened)
	cli.conns = []*conn{c}

	type callResult struct {
		buf buffer.Buffer
		err error
	}
	startCall := func(seq uint64) <-chan callResult {
		result := make(chan callResult, 1)
		go func() {
			buf, err := cli.Call(
				context.Background(),
				seq,
				buffer.NewNocopyBuffer([]byte("request")),
			)
			result <- callResult{buf: buf, err: err}
		}()
		return result
	}

	firstResult := startCall(101)
	firstMsg := <-c.queue
	if ok := c.doWrite(callLifecycleConn{}, firstMsg); !ok {
		t.Fatal("first doWrite failed")
	}

	secondResult := startCall(102)
	secondMsg := <-c.queue
	if ok := c.doWrite(callLifecycleConn{}, secondMsg); !ok {
		t.Fatal("second doWrite failed")
	}

	firstCall, ok := c.pending.extract(101)
	if !ok {
		t.Fatal("first pending call missing")
	}
	secondCall, ok := c.pending.extract(102)
	if !ok {
		t.Fatal("second pending call missing")
	}

	firstCall.deliver(buffer.NewNocopyBuffer([]byte("first")))
	secondCall.deliver(buffer.NewNocopyBuffer([]byte("second")))

	for _, tc := range []struct {
		name   string
		result <-chan callResult
		want   string
	}{
		{name: "first", result: firstResult, want: "first"},
		{name: "second", result: secondResult, want: "second"},
	} {
		select {
		case got := <-tc.result:
			if got.err != nil {
				t.Fatalf("%s Call error=%v", tc.name, got.err)
			}
			if got.buf == nil {
				t.Fatalf("%s Call buffer=nil", tc.name)
			}
			if string(got.buf.Bytes()) != tc.want {
				t.Fatalf("%s Call response=%q, want %q", tc.name, got.buf.Bytes(), tc.want)
			}
			got.buf.Release()
		case <-time.After(time.Second):
			t.Fatalf("%s Call did not complete", tc.name)
		}
	}
}

func TestCanceledQueuedCallDoesNotStopLaterWrites(t *testing.T) {
	cli := NewClient("", &Options{
		CallTimeout:    20 * time.Millisecond,
		WriteQueueSize: 8,
	})
	c := newConn(cli)
	c.state.Store(def.ConnOpened)
	cli.conns = []*conn{c}

	firstResult := make(chan error, 1)
	go func() {
		_, err := cli.Call(
			context.Background(),
			201,
			buffer.NewNocopyBuffer([]byte("first")),
		)
		firstResult <- err
	}()

	firstMsg := <-c.queue
	select {
	case err := <-firstResult:
		if err == nil {
			t.Fatal("first Call timeout error=nil")
		}
	case <-time.After(time.Second):
		t.Fatal("first Call did not time out")
	}

	if ok := c.doWrite(callLifecycleConn{}, firstMsg); !ok {
		t.Fatal("canceled queued Call stopped writer")
	}

	secondResult := make(chan struct {
		buf buffer.Buffer
		err error
	}, 1)
	go func() {
		buf, err := cli.Call(
			context.Background(),
			202,
			buffer.NewNocopyBuffer([]byte("second")),
		)
		secondResult <- struct {
			buf buffer.Buffer
			err error
		}{buf: buf, err: err}
	}()

	secondMsg := <-c.queue
	if ok := c.doWrite(callLifecycleConn{}, secondMsg); !ok {
		t.Fatal("second doWrite failed")
	}
	secondCall, ok := c.pending.extract(202)
	if !ok {
		t.Fatal("second pending call missing")
	}
	secondCall.deliver(buffer.NewNocopyBuffer([]byte("ok")))

	select {
	case got := <-secondResult:
		if got.err != nil {
			t.Fatalf("second Call error=%v", got.err)
		}
		if got.buf == nil || string(got.buf.Bytes()) != "ok" {
			t.Fatalf("second Call response=%v", got.buf)
		}
		got.buf.Release()
	case <-time.After(time.Second):
		t.Fatal("second Call did not complete")
	}
}
