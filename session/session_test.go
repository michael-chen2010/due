package session

import (
	"net"
	"sync"
	"testing"

	"github.com/dobyte/due/v2/network"
)

type testAttr struct {
	values sync.Map
}

func (a *testAttr) Set(key, value any) {
	a.values.Store(key, value)
}

func (a *testAttr) Get(key any) (any, bool) {
	return a.values.Load(key)
}

func (a *testAttr) Del(key any) bool {
	_, loaded := a.values.LoadAndDelete(key)
	return loaded
}

func (a *testAttr) Visit(fn func(key, value any) bool) {
	a.values.Range(fn)
}

type testConn struct {
	id     int64
	uid    int64
	attr   testAttr
	closed bool
}

var _ network.Conn = (*testConn)(nil)

func (c *testConn) ID() int64                     { return c.id }
func (c *testConn) UID() int64                    { return c.uid }
func (c *testConn) Attr() network.Attr            { return &c.attr }
func (c *testConn) Bind(uid int64)                { c.uid = uid }
func (c *testConn) Unbind()                       { c.uid = 0 }
func (c *testConn) Send([]byte) error             { return nil }
func (c *testConn) Push([]byte) error             { return nil }
func (c *testConn) State() network.ConnState      { return network.ConnOpened }
func (c *testConn) Close(...bool) error           { c.closed = true; return nil }
func (c *testConn) LocalIP() (string, error)      { return "127.0.0.1", nil }
func (c *testConn) LocalAddr() (net.Addr, error)  { return nil, nil }
func (c *testConn) RemoteIP() (string, error)     { return "127.0.0.1", nil }
func (c *testConn) RemoteAddr() (net.Addr, error) { return nil, nil }

func TestSessionTokenGenerationFencesReboundConnection(t *testing.T) {
	s := NewSession()
	const uid int64 = 1001
	first := &testConn{id: 11}
	second := &testConn{id: 12}
	s.AddConn(first)
	s.AddConn(second)

	if err := s.Bind(first.ID(), uid); err != nil {
		t.Fatalf("bind first connection: %v", err)
	}
	firstToken, err := s.Token(Conn, first.ID())
	if err != nil {
		t.Fatalf("first token: %v", err)
	}
	if firstToken.UID != uid || firstToken.Generation == 0 {
		t.Fatalf("first token = %+v, want uid=%d and non-zero generation", firstToken, uid)
	}
	if !s.IsCurrent(firstToken) {
		t.Fatalf("first token %+v must be current", firstToken)
	}

	if err := s.Bind(second.ID(), uid); err != nil {
		t.Fatalf("bind second connection: %v", err)
	}
	secondToken, err := s.Token(Conn, second.ID())
	if err != nil {
		t.Fatalf("second token: %v", err)
	}
	if secondToken.UID != uid {
		t.Fatalf("second token uid=%d, want %d", secondToken.UID, uid)
	}
	if secondToken.Generation <= firstToken.Generation {
		t.Fatalf("second generation=%d, want > first generation=%d", secondToken.Generation, firstToken.Generation)
	}
	if first.UID() != 0 {
		t.Fatalf("old connection uid=%d, want unbound", first.UID())
	}
	if s.IsCurrent(firstToken) {
		t.Fatalf("first token %+v must be stale after takeover", firstToken)
	}
	if !s.IsCurrent(secondToken) {
		t.Fatalf("second token %+v must be current", secondToken)
	}

	oldToken, err := s.Token(Conn, first.ID())
	if err != nil {
		t.Fatalf("old connection token after takeover: %v", err)
	}
	if oldToken != firstToken {
		t.Fatalf("old connection token=%+v, want original %+v", oldToken, firstToken)
	}
}

func TestSessionRepeatedBindKeepsGeneration(t *testing.T) {
	s := NewSession()
	conn := &testConn{id: 21}
	s.AddConn(conn)

	if err := s.Bind(conn.ID(), 2001); err != nil {
		t.Fatalf("first bind: %v", err)
	}
	first, err := s.Token(Conn, conn.ID())
	if err != nil {
		t.Fatalf("first token: %v", err)
	}

	if err := s.Bind(conn.ID(), 2001); err != nil {
		t.Fatalf("repeat bind: %v", err)
	}
	second, err := s.Token(Conn, conn.ID())
	if err != nil {
		t.Fatalf("second token: %v", err)
	}
	if second != first {
		t.Fatalf("repeat bind token=%+v, want unchanged %+v", second, first)
	}
}

func TestSessionGenerationSurvivesUnbind(t *testing.T) {
	s := NewSession()
	const uid int64 = 3001
	first := &testConn{id: 31}
	second := &testConn{id: 32}
	s.AddConn(first)
	s.AddConn(second)

	if err := s.Bind(first.ID(), uid); err != nil {
		t.Fatalf("bind first: %v", err)
	}
	firstToken, err := s.Token(User, uid)
	if err != nil {
		t.Fatalf("first user token: %v", err)
	}

	cid, err := s.Unbind(uid)
	if err != nil {
		t.Fatalf("unbind: %v", err)
	}
	if cid != first.ID() {
		t.Fatalf("unbind cid=%d, want %d", cid, first.ID())
	}
	if s.IsCurrent(firstToken) {
		t.Fatalf("token %+v must not remain current after unbind", firstToken)
	}

	if err := s.Bind(second.ID(), uid); err != nil {
		t.Fatalf("bind second: %v", err)
	}
	secondToken, err := s.Token(User, uid)
	if err != nil {
		t.Fatalf("second user token: %v", err)
	}
	if secondToken.Generation <= firstToken.Generation {
		t.Fatalf("generation after unbind=%d, want > %d", secondToken.Generation, firstToken.Generation)
	}
}

func TestSessionTokenZeroIsNeverCurrent(t *testing.T) {
	s := NewSession()
	if s.IsCurrent(Token{}) {
		t.Fatal("zero token must never be current")
	}
}

func TestSessionBindTokenInstallsExternallyAllocatedToken(t *testing.T) {
	s := NewSession()
	const uid int64 = 4001
	first := &testConn{id: 41}
	second := &testConn{id: 42}
	s.AddConn(first)
	s.AddConn(second)

	firstToken := Token{UID: uid, Generation: 101}
	if err := s.BindToken(first.ID(), uid, firstToken); err != nil {
		t.Fatalf("bind first token: %v", err)
	}
	if got, err := s.Token(Conn, first.ID()); err != nil || got != firstToken {
		t.Fatalf("first token=%+v err=%v, want %+v", got, err, firstToken)
	}

	secondToken := Token{UID: uid, Generation: 102}
	if err := s.BindToken(second.ID(), uid, secondToken); err != nil {
		t.Fatalf("bind second token: %v", err)
	}
	if first.UID() != 0 {
		t.Fatalf("old connection uid=%d, want unbound", first.UID())
	}
	if got, err := s.Token(User, uid); err != nil || got != secondToken {
		t.Fatalf("current token=%+v err=%v, want %+v", got, err, secondToken)
	}
	if s.IsCurrent(firstToken) {
		t.Fatalf("first token %+v must be stale locally", firstToken)
	}
	if !s.IsCurrent(secondToken) {
		t.Fatalf("second token %+v must be current locally", secondToken)
	}
}

func TestSessionBindTokenRejectsInvalidToken(t *testing.T) {
	s := NewSession()
	conn := &testConn{id: 51}
	s.AddConn(conn)

	for _, token := range []Token{
		{},
		{UID: 5002, Generation: 1},
		{UID: 5001, Generation: 0},
	} {
		if err := s.BindToken(conn.ID(), 5001, token); err == nil {
			t.Fatalf("BindToken accepted invalid token %+v", token)
		}
	}
}
