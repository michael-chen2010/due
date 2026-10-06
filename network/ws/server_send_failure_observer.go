package ws

import (
	"net"

	"github.com/dobyte/due/v2/errors"
)

const (
	SendFailureQueueBytesExceeded = "queue_bytes_exceeded"
	SendFailureEnqueueTimeout     = "enqueue_timeout"
	SendFailureSocketWriteTimeout = "socket_write_timeout"
	SendFailureSocketWriteError   = "socket_write_error"
)

type ServerSendFailureObserver func(reason string)

func (o ServerSendFailureObserver) observe(reason string) {
	if o == nil {
		return
	}
	defer func() {
		_ = recover()
	}()
	o(reason)
}

func (c *serverConn) observeSendFailure(reason string) {
	if c == nil || c.connMgr == nil || c.connMgr.server == nil {
		return
	}
	c.connMgr.server.opts.sendFailureObserver.observe(reason)
}

func socketWriteFailureReason(err error) string {
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return SendFailureSocketWriteTimeout
	}
	return SendFailureSocketWriteError
}
