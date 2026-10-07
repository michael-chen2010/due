package gate

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dobyte/due/v2/network"
	"github.com/dobyte/due/v2/session"
)

type disconnectBindRaceAttr struct {
	values sync.Map
}

func (a *disconnectBindRaceAttr) Set(key, value any) {
	a.values.Store(key, value)
}

func (a *disconnectBindRaceAttr) Get(key any) (any, bool) {
	return a.values.Load(key)
}

func (a *disconnectBindRaceAttr) Del(key any) bool {
	_, loaded := a.values.LoadAndDelete(key)
	return loaded
}

func (a *disconnectBindRaceAttr) Visit(fn func(key, value any) bool) {
	a.values.Range(fn)
}

type disconnectBindRaceConn struct {
	id int64

	uid atomic.Int64

	uidCalls      atomic.Int32
	detachEntered chan struct{}
	releaseDetach chan struct{}
	detachOnce    sync.Once
	attr          disconnectBindRaceAttr
}

var _ network.Conn = (*disconnectBindRaceConn)(nil)

func newDisconnectBindRaceConn(id int64) *disconnectBindRaceConn {
	return &disconnectBindRaceConn{
		id:            id,
		detachEntered: make(chan struct{}),
		releaseDetach: make(chan struct{}),
	}
}

func (c *disconnectBindRaceConn) ID() int64 { return c.id }

func (c *disconnectBindRaceConn) UID() int64 {
	call := c.uidCalls.Add(1)
	// AddConn performs the first UID read. The second read is the disconnect
	// snapshot/removal. Hold it open so a concurrent bind attempts to race it.
	if call == 2 {
		c.detachOnce.Do(func() { close(c.detachEntered) })
		<-c.releaseDetach
	}
	return c.uid.Load()
}

func (c *disconnectBindRaceConn) Attr() network.Attr { return &c.attr }
func (c *disconnectBindRaceConn) Bind(uid int64)     { c.uid.Store(uid) }
func (c *disconnectBindRaceConn) Unbind()            { c.uid.Store(0) }
func (c *disconnectBindRaceConn) Send([]byte) error  { return nil }
func (c *disconnectBindRaceConn) Push([]byte) error  { return nil }
func (c *disconnectBindRaceConn) State() network.ConnState {
	return network.ConnOpened
}
func (c *disconnectBindRaceConn) Close(...bool) error           { return nil }
func (c *disconnectBindRaceConn) LocalIP() (string, error)      { return "127.0.0.1", nil }
func (c *disconnectBindRaceConn) LocalAddr() (net.Addr, error)  { return nil, nil }
func (c *disconnectBindRaceConn) RemoteIP() (string, error)     { return "127.0.0.1", nil }
func (c *disconnectBindRaceConn) RemoteAddr() (net.Addr, error) { return nil, nil }

func TestDisconnectRemovalSerializesConcurrentOwnershipBind(t *testing.T) {
	const (
		cid int64 = 9101
		uid int64 = 9201
	)

	store := session.NewMemoryOwnershipStore()
	manager := session.NewSession()
	conn := newDisconnectBindRaceConn(cid)
	manager.AddConn(conn)
	g := &Gate{
		opts:    &options{ownershipStore: store},
		session: manager,
	}

	type removedState struct {
		uid   int64
		token session.Token
	}
	removed := make(chan removedState, 1)
	go func() {
		removedUID, token := manager.RemConnWithToken(conn)
		removed <- removedState{uid: removedUID, token: token}
	}()

	select {
	case <-conn.detachEntered:
	case <-time.After(time.Second):
		t.Fatal("disconnect removal did not enter atomic UID snapshot")
	}

	bindDone := make(chan error, 1)
	go func() {
		_, err := g.bindSession(context.Background(), cid, uid)
		bindDone <- err
	}()

	select {
	case err := <-bindDone:
		t.Fatalf("concurrent bind completed while disconnect held session removal lock: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	if _, ok, err := store.Current(context.Background(), uid); err != nil {
		t.Fatalf("read ownership while disconnect is blocked: %v", err)
	} else if ok {
		t.Fatal("concurrent bind acquired ownership before atomic disconnect removal completed")
	}

	close(conn.releaseDetach)

	select {
	case got := <-removed:
		if got.uid != 0 || got.token != (session.Token{}) {
			t.Fatalf("removed state uid/token=%d/%+v, want unbound zero state", got.uid, got.token)
		}
	case <-time.After(time.Second):
		t.Fatal("disconnect removal did not finish")
	}

	select {
	case err := <-bindDone:
		if err == nil {
			t.Fatal("bind unexpectedly succeeded after connection removal")
		}
	case <-time.After(time.Second):
		t.Fatal("concurrent bind did not finish after disconnect removal")
	}

	if _, ok, err := store.Current(context.Background(), uid); err != nil {
		t.Fatalf("read final ownership: %v", err)
	} else if ok {
		t.Fatal("disconnect/bind race leaked session ownership")
	}
}
