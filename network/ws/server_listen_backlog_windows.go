//go:build windows

package ws

import (
	"net"
	"syscall"
)

func applyServerListenBacklog(listener *net.TCPListener, backlog int) error {
	raw, err := listener.SyscallConn()
	if err != nil {
		return err
	}

	var listenErr error
	if err := raw.Control(func(fd uintptr) {
		listenErr = syscall.Listen(syscall.Handle(fd), backlog)
	}); err != nil {
		return err
	}
	return listenErr
}
