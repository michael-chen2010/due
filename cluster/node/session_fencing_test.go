package node

import (
	"context"
	stderrors "errors"
	"sync/atomic"
	"testing"

	"github.com/dobyte/due/v2/session"
)

func deliverAndHandleForSessionFencingTest(
	t *testing.T,
	node *Node,
	gid string,
	uid int64,
	token session.Token,
	route int32,
) {
	t.Helper()
	node.router.deliver(gid, node.opts.id, "", 1, uid, token, 1, route, nil)
	req := <-node.router.receive()
	node.router.handle(req)
}

func TestStatefulRouteRejectsStaleSessionBeforeHandler(t *testing.T) {
	store := session.NewMemoryOwnershipStore()
	const uid int64 = 1001

	stale, err := store.Acquire(context.Background(), uid)
	if err != nil {
		t.Fatalf("acquire stale token: %v", err)
	}
	current, err := store.Acquire(context.Background(), uid)
	if err != nil {
		t.Fatalf("acquire current token: %v", err)
	}
	if stale == current {
		t.Fatal("expected distinct session generations")
	}

	node := NewNode(WithOwnershipStore(store))
	var calls atomic.Int32
	node.router.AddRouteHandler(9101, func(Context) {
		calls.Add(1)
	}, StatefulRoute)

	deliverAndHandleForSessionFencingTest(t, node, "gate-1", uid, stale, 9101)

	if got := calls.Load(); got != 0 {
		t.Fatalf("stale Stateful Route invoked handler %d times, want 0", got)
	}
}

func TestStatefulRouteAllowsCurrentSession(t *testing.T) {
	store := session.NewMemoryOwnershipStore()
	const uid int64 = 1002

	current, err := store.Acquire(context.Background(), uid)
	if err != nil {
		t.Fatalf("acquire current token: %v", err)
	}

	node := NewNode(WithOwnershipStore(store))
	var calls atomic.Int32
	node.router.AddRouteHandler(9101, func(ctx Context) {
		if got := ctx.SessionToken(); got != current {
			t.Fatalf("handler SessionToken=%+v, want %+v", got, current)
		}
		calls.Add(1)
	}, StatefulRoute)

	deliverAndHandleForSessionFencingTest(t, node, "gate-1", uid, current, 9101)

	if got := calls.Load(); got != 1 {
		t.Fatalf("current Stateful Route invoked handler %d times, want 1", got)
	}
}

func TestStatelessRouteDoesNotRequireCurrentSession(t *testing.T) {
	store := session.NewMemoryOwnershipStore()
	const uid int64 = 1003

	stale, err := store.Acquire(context.Background(), uid)
	if err != nil {
		t.Fatalf("acquire stale token: %v", err)
	}
	if _, err = store.Acquire(context.Background(), uid); err != nil {
		t.Fatalf("advance current token: %v", err)
	}

	node := NewNode(WithOwnershipStore(store))
	var calls atomic.Int32
	node.router.AddRouteHandler(9100, func(Context) {
		calls.Add(1)
	})

	deliverAndHandleForSessionFencingTest(t, node, "gate-1", uid, stale, 9100)

	if got := calls.Load(); got != 1 {
		t.Fatalf("Stateless Route invoked handler %d times, want 1", got)
	}
}

type errorOwnershipStore struct {
	err error
}

func (s errorOwnershipStore) Acquire(context.Context, int64) (session.Token, error) {
	return session.Token{}, s.err
}

func (s errorOwnershipStore) Current(context.Context, int64) (session.Token, bool, error) {
	return session.Token{}, false, s.err
}

func (s errorOwnershipStore) Release(context.Context, int64, session.Token) (bool, error) {
	return false, s.err
}

func TestStatefulRouteOwnershipLookupErrorRejectsBeforePreRouteAndHandler(t *testing.T) {
	storeErr := stderrors.New("ownership unavailable")
	node := NewNode(WithOwnershipStore(errorOwnershipStore{err: storeErr}))

	var preCalls atomic.Int32
	var handlerCalls atomic.Int32
	node.router.SetPreRouteHandler(func(Context) {
		preCalls.Add(1)
	})
	node.router.AddRouteHandler(9101, func(Context) {
		handlerCalls.Add(1)
	}, StatefulRoute)

	const uid int64 = 1004
	deliverAndHandleForSessionFencingTest(t, node, "gate-1", uid, session.Token{UID: uid, Generation: 1}, 9101)

	if got := preCalls.Load(); got != 0 {
		t.Fatalf("ownership lookup error invoked pre-route handler %d times, want 0", got)
	}
	if got := handlerCalls.Load(); got != 0 {
		t.Fatalf("ownership lookup error invoked route handler %d times, want 0", got)
	}
}

func TestInternalStatefulRouteDoesNotRequireSessionToken(t *testing.T) {
	store := session.NewMemoryOwnershipStore()
	node := NewNode(WithOwnershipStore(store))

	var calls atomic.Int32
	node.router.AddRouteHandler(9201, func(Context) {
		calls.Add(1)
	}, StatefulRoute)

	deliverAndHandleForSessionFencingTest(t, node, "", 1005, session.Token{}, 9201)

	if got := calls.Load(); got != 1 {
		t.Fatalf("internal Stateful Route invoked handler %d times, want 1", got)
	}
}
