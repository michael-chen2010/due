package gate

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/dobyte/due/v2/network"
	"github.com/dobyte/due/v2/session"
)

type ownershipTestAttr struct {
	values sync.Map
}

func (a *ownershipTestAttr) Set(key, value any) {
	a.values.Store(key, value)
}

func (a *ownershipTestAttr) Get(key any) (any, bool) {
	return a.values.Load(key)
}

func (a *ownershipTestAttr) Del(key any) bool {
	_, loaded := a.values.LoadAndDelete(key)
	return loaded
}

func (a *ownershipTestAttr) Visit(fn func(key, value any) bool) {
	a.values.Range(fn)
}

type ownershipTestConn struct {
	id   int64
	uid  int64
	attr ownershipTestAttr
}

var _ network.Conn = (*ownershipTestConn)(nil)

func (c *ownershipTestConn) ID() int64                     { return c.id }
func (c *ownershipTestConn) UID() int64                    { return c.uid }
func (c *ownershipTestConn) Attr() network.Attr            { return &c.attr }
func (c *ownershipTestConn) Bind(uid int64)                { c.uid = uid }
func (c *ownershipTestConn) Unbind()                       { c.uid = 0 }
func (c *ownershipTestConn) Send([]byte) error             { return nil }
func (c *ownershipTestConn) Push([]byte) error             { return nil }
func (c *ownershipTestConn) State() network.ConnState      { return network.ConnOpened }
func (c *ownershipTestConn) Close(...bool) error           { return nil }
func (c *ownershipTestConn) LocalIP() (string, error)      { return "127.0.0.1", nil }
func (c *ownershipTestConn) LocalAddr() (net.Addr, error)  { return nil, nil }
func (c *ownershipTestConn) RemoteIP() (string, error)     { return "127.0.0.1", nil }
func (c *ownershipTestConn) RemoteAddr() (net.Addr, error) { return nil, nil }

func newOwnershipTestGate(store session.OwnershipStore, cid int64) (*Gate, *ownershipTestConn) {
	manager := session.NewSession()
	conn := &ownershipTestConn{id: cid}
	manager.AddConn(conn)

	return &Gate{
		opts: &options{
			ownershipStore: store,
		},
		session: manager,
	}, conn
}

func TestWithOwnershipStore(t *testing.T) {
	store := session.NewMemoryOwnershipStore()
	opts := defaultOptions()
	WithOwnershipStore(store)(opts)
	if opts.ownershipStore != store {
		t.Fatal("ownership store option was not applied")
	}
}

func TestGateBindSessionUsesSharedOwnershipStore(t *testing.T) {
	store := session.NewMemoryOwnershipStore()
	firstGate, _ := newOwnershipTestGate(store, 101)
	secondGate, _ := newOwnershipTestGate(store, 201)
	ctx := context.Background()
	const uid int64 = 6001

	first, err := firstGate.bindSession(ctx, 101, uid)
	if err != nil {
		t.Fatalf("first gate bind: %v", err)
	}
	repeated, err := firstGate.bindSession(ctx, 101, uid)
	if err != nil {
		t.Fatalf("repeat first gate bind: %v", err)
	}
	if repeated != first {
		t.Fatalf("repeat bind token=%+v, want unchanged %+v", repeated, first)
	}

	second, err := secondGate.bindSession(ctx, 201, uid)
	if err != nil {
		t.Fatalf("second gate bind: %v", err)
	}
	if second.Generation <= first.Generation {
		t.Fatalf("second generation=%d, want > %d", second.Generation, first.Generation)
	}

	rebound, err := firstGate.bindSession(ctx, 101, uid)
	if err != nil {
		t.Fatalf("stale first gate rebind: %v", err)
	}
	if rebound.Generation <= second.Generation {
		t.Fatalf("rebound generation=%d, want > %d", rebound.Generation, second.Generation)
	}
	if got, err := firstGate.session.Token(session.Conn, 101); err != nil || got != rebound {
		t.Fatalf("first gate local token=%+v err=%v, want %+v", got, err, rebound)
	}
	current, ok, err := store.Current(ctx, uid)
	if err != nil || !ok || current != rebound {
		t.Fatalf("store current=%+v found=%v err=%v, want %+v", current, ok, err, rebound)
	}
}

func TestGateReleaseSessionDoesNotDeleteNewOwner(t *testing.T) {
	store := session.NewMemoryOwnershipStore()
	firstGate, _ := newOwnershipTestGate(store, 301)
	secondGate, _ := newOwnershipTestGate(store, 401)
	ctx := context.Background()
	const uid int64 = 7001

	stale, err := firstGate.bindSession(ctx, 301, uid)
	if err != nil {
		t.Fatalf("first gate bind: %v", err)
	}
	current, err := secondGate.bindSession(ctx, 401, uid)
	if err != nil {
		t.Fatalf("second gate bind: %v", err)
	}

	released, err := firstGate.releaseSession(ctx, stale)
	if err != nil {
		t.Fatalf("release stale: %v", err)
	}
	if released {
		t.Fatal("stale gate unexpectedly released current ownership")
	}

	got, ok, err := store.Current(ctx, uid)
	if err != nil || !ok || got != current {
		t.Fatalf("current after stale release=%+v found=%v err=%v, want %+v", got, ok, err, current)
	}

	released, err = secondGate.releaseSession(ctx, current)
	if err != nil || !released {
		t.Fatalf("release current released=%v err=%v", released, err)
	}
}

func TestGateBindSessionFallsBackToLocalGeneration(t *testing.T) {
	gate, _ := newOwnershipTestGate(nil, 501)
	const uid int64 = 8001

	token, err := gate.bindSession(context.Background(), 501, uid)
	if err != nil {
		t.Fatalf("local bind: %v", err)
	}
	if token.UID != uid || token.Generation == 0 {
		t.Fatalf("local token=%+v", token)
	}
}
