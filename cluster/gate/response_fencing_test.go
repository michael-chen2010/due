package gate

import (
	"context"
	"net"
	"sync"
	"testing"

	dueerrors "github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/network"
	"github.com/dobyte/due/v2/session"
)

type responseFenceAttr struct {
	values sync.Map
}

func (a *responseFenceAttr) Set(key, value any)      { a.values.Store(key, value) }
func (a *responseFenceAttr) Get(key any) (any, bool) { return a.values.Load(key) }
func (a *responseFenceAttr) Del(key any) bool {
	_, existed := a.values.LoadAndDelete(key)
	return existed
}
func (a *responseFenceAttr) Visit(fn func(any, any) bool) { a.values.Range(fn) }

type responseFenceConn struct {
	id     int64
	uid    int64
	attr   responseFenceAttr
	sends  int
	pushes int
}

var _ network.Conn = (*responseFenceConn)(nil)

func (c *responseFenceConn) ID() int64                     { return c.id }
func (c *responseFenceConn) UID() int64                    { return c.uid }
func (c *responseFenceConn) Attr() network.Attr            { return &c.attr }
func (c *responseFenceConn) Bind(uid int64)                { c.uid = uid }
func (c *responseFenceConn) Unbind()                       { c.uid = 0 }
func (c *responseFenceConn) Send([]byte) error             { c.sends++; return nil }
func (c *responseFenceConn) Push([]byte) error             { c.pushes++; return nil }
func (c *responseFenceConn) State() network.ConnState      { return network.ConnOpened }
func (c *responseFenceConn) Close(...bool) error           { return nil }
func (c *responseFenceConn) LocalIP() (string, error)      { return "127.0.0.1", nil }
func (c *responseFenceConn) LocalAddr() (net.Addr, error)  { return nil, nil }
func (c *responseFenceConn) RemoteIP() (string, error)     { return "127.0.0.1", nil }
func (c *responseFenceConn) RemoteAddr() (net.Addr, error) { return nil, nil }

func TestProviderPushRejectsResponseForStaleSessionToken(t *testing.T) {
	const uid int64 = 9001
	store := session.NewMemoryOwnershipStore()
	manager := session.NewSession()
	oldConn := &responseFenceConn{id: 101}
	newConn := &responseFenceConn{id: 102}
	manager.AddConn(oldConn)
	manager.AddConn(newConn)
	g := &Gate{opts: &options{ownershipStore: store}, session: manager}

	oldToken, err := g.bindSession(context.Background(), oldConn.id, uid)
	if err != nil {
		t.Fatalf("bind old session: %v", err)
	}
	newToken, err := g.bindSession(context.Background(), newConn.id, uid)
	if err != nil {
		t.Fatalf("bind replacement session: %v", err)
	}
	if newToken.Generation <= oldToken.Generation {
		t.Fatalf("new generation=%d, want > old generation=%d", newToken.Generation, oldToken.Generation)
	}

	p := &provider{gate: g}
	if err := p.Push(context.Background(), session.Conn, oldConn.id, false, oldToken, []byte("old response")); !dueerrors.Is(err, dueerrors.ErrStaleSession) {
		t.Fatalf("stale response push error=%v, want %v", err, dueerrors.ErrStaleSession)
	}
	if oldConn.sends != 0 || oldConn.pushes != 0 || newConn.sends != 0 || newConn.pushes != 0 {
		t.Fatalf(
			"stale response delivered: old sends=%d pushes=%d new sends=%d pushes=%d",
			oldConn.sends,
			oldConn.pushes,
			newConn.sends,
			newConn.pushes,
		)
	}

	if err := p.Push(context.Background(), session.Conn, newConn.id, false, newToken, []byte("current response")); err != nil {
		t.Fatalf("current response push: %v", err)
	}
	if oldConn.sends != 0 || oldConn.pushes != 0 || newConn.sends != 1 || newConn.pushes != 0 {
		t.Fatalf(
			"current response delivery: old sends=%d pushes=%d new sends=%d pushes=%d; want response on Send only",
			oldConn.sends,
			oldConn.pushes,
			newConn.sends,
			newConn.pushes,
		)
	}
}

func TestProviderPushSupportsFencedUserDeliveryAndRejectsStaleGeneration(t *testing.T) {
	const uid int64 = 9101
	store := session.NewMemoryOwnershipStore()
	manager := session.NewSession()
	oldConn := &responseFenceConn{id: 301}
	newConn := &responseFenceConn{id: 302}
	manager.AddConn(oldConn)
	manager.AddConn(newConn)
	g := &Gate{opts: &options{ownershipStore: store}, session: manager}
	p := &provider{gate: g}

	oldToken, err := g.bindSession(context.Background(), oldConn.id, uid)
	if err != nil {
		t.Fatalf("bind old session: %v", err)
	}
	if err := p.Push(
		context.Background(),
		session.User,
		uid,
		false,
		oldToken,
		[]byte("current full sync"),
	); err != nil {
		t.Fatalf("current fenced user push: %v", err)
	}
	if oldConn.sends != 1 || oldConn.pushes != 0 {
		t.Fatalf(
			"current fenced user delivery: sends=%d pushes=%d; want Send only",
			oldConn.sends,
			oldConn.pushes,
		)
	}

	newToken, err := g.bindSession(context.Background(), newConn.id, uid)
	if err != nil {
		t.Fatalf("bind replacement session: %v", err)
	}
	if newToken.Generation <= oldToken.Generation {
		t.Fatalf(
			"new generation=%d, want > old generation=%d",
			newToken.Generation,
			oldToken.Generation,
		)
	}
	if err := p.Push(
		context.Background(),
		session.User,
		uid,
		false,
		oldToken,
		[]byte("stale full sync"),
	); !dueerrors.Is(err, dueerrors.ErrStaleSession) {
		t.Fatalf(
			"stale fenced user push error=%v, want %v",
			err,
			dueerrors.ErrStaleSession,
		)
	}
	if oldConn.sends != 1 || oldConn.pushes != 0 ||
		newConn.sends != 0 || newConn.pushes != 0 {
		t.Fatalf(
			"stale fenced user delivery: old sends=%d pushes=%d new sends=%d pushes=%d",
			oldConn.sends,
			oldConn.pushes,
			newConn.sends,
			newConn.pushes,
		)
	}
}

func TestProviderPushKeepsOrdinaryPushOnLowPriorityQueue(t *testing.T) {
	manager := session.NewSession()
	conn := &responseFenceConn{id: 201}
	manager.AddConn(conn)
	g := &Gate{opts: &options{}, session: manager}

	p := &provider{gate: g}
	if err := p.Push(context.Background(), session.Conn, conn.id, false, session.Token{}, []byte("ordinary push")); err != nil {
		t.Fatalf("ordinary push: %v", err)
	}
	if conn.sends != 0 || conn.pushes != 1 {
		t.Fatalf(
			"ordinary push delivery: sends=%d pushes=%d; want Push only",
			conn.sends,
			conn.pushes,
		)
	}
}
