//go:build !windows && !aix && !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris

package ws

import (
	"fmt"
	"net"
)

func applyServerListenBacklog(_ *net.TCPListener, backlog int) error {
	return fmt.Errorf("websocket listen backlog %d is not supported on this platform", backlog)
}
