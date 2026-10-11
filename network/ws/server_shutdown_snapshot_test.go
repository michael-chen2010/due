package ws

import (
	"github.com/gorilla/websocket"
	"sync"
	"sync/atomic"
	"testing"
)

func TestPartitionShutdownSnapshotIsSafeDuringRecycle(t *testing.T) {
	p := &partition{connections: make(map[*websocket.Conn]*serverConn)}
	wire := &websocket.Conn{}
	conn := &serverConn{}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			p.store(wire, conn)
			p.delete(wire)
		}
	}()
	for i := 0; i < 2000; i++ {
		for _, current := range p.snapshot() {
			if current != conn {
				t.Fatal("snapshot changed connection identity")
			}
		}
	}
	wg.Wait()
}

func TestPartitionDoesNotRegisterLateUpgradeAfterShutdown(t *testing.T) {
	p := &partition{connections: make(map[*websocket.Conn]*serverConn)}
	var closing atomic.Bool
	closing.Store(true)
	if p.storeOpen(&websocket.Conn{}, &serverConn{}, &closing) || len(p.snapshot()) != 0 {
		t.Fatal("late upgrade registered after closing snapshot")
	}
}
func TestShutdownConnectionManagerRejectsNewReservations(t *testing.T) {
	s := NewServer(WithServerMaxConnNum(4)).(*server)
	s.connMgr.close()
	if s.connMgr.reserve() {
		t.Fatal("stopped manager accepts new connection reservations")
	}
}
