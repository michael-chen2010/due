//go:build windows

package ws

import (
	"net"
	"syscall"
)

func windowsListenBacklog(backlog int) int {
	// winsock2.h defines SOMAXCONN_HINT(N) as -(N). The Microsoft TCP/IP
	// provider uses this encoding to request a backlog above SOMAXCONN's
	// "reasonable maximum" while still clamping N to its supported range.
	return -backlog
}

func applyServerListenBacklog(listener *net.TCPListener, backlog int) error {
	raw, err := listener.SyscallConn()
	if err != nil {
		return err
	}

	var listenErr error
	if err := raw.Control(func(fd uintptr) {
		listenErr = syscall.Listen(syscall.Handle(fd), windowsListenBacklog(backlog))
	}); err != nil {
		return err
	}
	return listenErr
}
