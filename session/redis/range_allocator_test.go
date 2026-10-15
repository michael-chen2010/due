package redis_test

import (
	"context"
	"errors"
	"fmt"
	goredis "github.com/redis/go-redis/v9"
	"math"
	"sync"
	"testing"

	sessionredis "github.com/dobyte/due/session/redis/v2"
	"github.com/dobyte/due/v2/session"
)

// Test-only durable model. State deliberately outlives deletion of Redis keys;
// real cross-process durability is verified by WebGameServer's SQL allocator.
type rangeModel struct {
	mu       sync.Mutex
	reserved map[int64]uint64
}

func newRangeModel() *rangeModel { return &rangeModel{reserved: map[int64]uint64{}} }
func (m *rangeModel) WithGenerationRange(ctx context.Context, uid int64, floor uint64, use func(context.Context, uint64, uint64) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e := ctx.Err(); e != nil {
		return e
	}
	if m.reserved[uid] > floor {
		floor = m.reserved[uid]
	}
	if floor >= math.MaxInt64 {
		return session.ErrOwnershipGenerationExhausted
	}
	size := uint64(1024)
	if size > math.MaxInt64-floor {
		size = math.MaxInt64 - floor
	}
	m.reserved[uid] = floor + size
	return use(ctx, floor+1, floor+size)
}

type rangeFunc func(context.Context, int64, uint64, func(context.Context, uint64, uint64) error) error

func (f rangeFunc) WithGenerationRange(ctx context.Context, u int64, floor uint64, use func(context.Context, uint64, uint64) error) error {
	return f(ctx, u, floor, use)
}

func TestRedisOwnershipRequiresRangeAndChecksAllocatorContract(t *testing.T) {
	first, _, cleanup := newStores(t)
	defer cleanup()
	ctx := context.Background()
	const uid int64 = 8001
	// NewStore without an allocator may read/release, but cannot bootstrap a
	// fresh or legacy/exhausted hash by silently starting generation at one.
	reader := sessionredis.NewStore(sessionredis.WithAddrs(integrationAddress(t)), sessionredis.WithPrefix(prefixOf(first)))
	defer reader.Close()
	if token, e := reader.Acquire(ctx, uid); !errors.Is(e, sessionredis.ErrGenerationRangeRequired) || token != (session.Token{}) {
		t.Fatal("unsafe bootstrap allowed", token, e)
	}
	for _, bad := range []rangeFunc{
		func(context.Context, int64, uint64, func(context.Context, uint64, uint64) error) error { return nil },
		func(ctx context.Context, _ int64, _ uint64, use func(context.Context, uint64, uint64) error) error {
			return use(ctx, 0, 1024)
		},
		func(ctx context.Context, _ int64, _ uint64, use func(context.Context, uint64, uint64) error) error {
			return use(ctx, 10, 9)
		},
	} {
		s := sessionredis.NewStore(sessionredis.WithAddrs(integrationAddress(t)), sessionredis.WithPrefix(prefixOf(first)), sessionredis.WithGenerationRangeAllocator(bad))
		token, e := s.Acquire(ctx, uid)
		s.Close()
		if e == nil || token != (session.Token{}) {
			t.Fatal("invalid allocator issued token", token, e)
		}
	}
	wanted := errors.New("allocation failed after callback")
	failure := rangeFunc(func(ctx context.Context, _ int64, _ uint64, use func(context.Context, uint64, uint64) error) error {
		if e := use(ctx, 1, 1024); e != nil {
			return e
		}
		return wanted
	})
	s := sessionredis.NewStore(sessionredis.WithAddrs(integrationAddress(t)), sessionredis.WithPrefix(prefixOf(first)), sessionredis.WithGenerationRangeAllocator(failure))
	defer s.Close()
	if token, e := s.Acquire(ctx, uid); !errors.Is(e, wanted) || token != (session.Token{}) {
		t.Fatal("allocator error issued token", token, e)
	}
	// Already-reserved ranges remain usable through a read/release client; no
	// new reservation may be invented without the allocator.
	if token, e := reader.Acquire(ctx, uid); e != nil || token.Generation != 2 {
		t.Fatal("existing reserved range not usable", token, e)
	}
}

func TestRedisOwnershipRangePreservesLargeExactGenerations(t *testing.T) {
	first, _, cleanup := newStores(t)
	defer cleanup()
	ctx := context.Background()
	m := newRangeModel()
	m.reserved[9001] = uint64(1<<53) + 17
	s := sessionredis.NewStore(sessionredis.WithAddrs(integrationAddress(t)), sessionredis.WithPrefix(prefixOf(first)), sessionredis.WithGenerationRangeAllocator(m))
	defer s.Close()
	for i := uint64(1); i <= 3; i++ {
		token, e := s.Acquire(ctx, 9001)
		if e != nil || token.Generation != uint64(1<<53)+17+i {
			t.Fatal("generation lost precision", token, e)
		}
	}
	m.reserved[9002] = math.MaxInt64 - 1
	token, e := s.Acquire(ctx, 9002)
	if e != nil || token.Generation != math.MaxInt64 {
		t.Fatal("last generation unavailable", token, e)
	}
	if token, e := s.Acquire(ctx, 9002); !errors.Is(e, session.ErrOwnershipGenerationExhausted) || token != (session.Token{}) {
		t.Fatal("exhaustion reused generation", token, e)
	}
}

func TestRedisOwnershipLegacyFloorAndExhaustedBlock(t *testing.T) {
	first, _, cleanup := newStores(t)
	defer cleanup()
	ctx := context.Background()
	const uid int64 = 10042
	client := goredis.NewClient(&goredis.Options{Addr: integrationAddress(t)})
	defer client.Close()
	prefix := prefixOf(first)
	key := fmt.Sprintf("%s:%d", prefix, uid)
	if e := client.HSet(ctx, key, "generation", "100", "current", "100").Err(); e != nil {
		t.Fatal(e)
	}
	s := sessionredis.NewStore(sessionredis.WithAddrs(integrationAddress(t)), sessionredis.WithPrefix(prefix), sessionredis.WithGenerationRangeAllocator(newRangeModel()))
	defer s.Close()
	token, e := s.Acquire(ctx, uid)
	if e != nil || token.Generation != 101 {
		t.Fatal("legacy floor ignored", token, e)
	}
	if released, e := s.Release(ctx, uid, session.Token{UID: uid, Generation: 100}); e != nil || released {
		t.Fatal("legacy old token released replacement", released, e)
	}
	if e := client.HSet(ctx, key, "generation", "1124", "current", "1124").Err(); e != nil {
		t.Fatal(e)
	}
	token, e = s.Acquire(ctx, uid)
	if e != nil || token.Generation != 1125 {
		t.Fatal("exhausted block not advanced", token, e)
	}
	if e := client.HSet(ctx, key, "generation", "01", "current", "01").Err(); e != nil {
		t.Fatal(e)
	}
	if token, e := s.Acquire(ctx, uid); e == nil || token != (session.Token{}) {
		t.Fatal("malformed generation issued token", token, e)
	}
	if token, found, e := s.Current(ctx, uid); e == nil || found || token != (session.Token{}) {
		t.Fatal("malformed current accepted", token, found, e)
	}
}
