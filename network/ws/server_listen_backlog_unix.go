//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

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
		listenErr = syscall.Listen(int(fd), backlog)
	}); err != nil {
		return err
	}
	return listenErr
}
