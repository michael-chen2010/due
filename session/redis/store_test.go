package redis_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	sessionredis "github.com/dobyte/due/session/redis/v2"
	"github.com/dobyte/due/v2/session"
	goredis "github.com/redis/go-redis/v9"
)

func integrationAddress(t *testing.T) string {
	t.Helper()
	if os.Getenv("DUE_SESSION_REDIS_INTEGRATION") != "1" {
		t.Skip("set DUE_SESSION_REDIS_INTEGRATION=1 to run Redis ownership integration tests")
	}
	if addr := os.Getenv("DUE_REDIS"); addr != "" {
		return addr
	}
	return "127.0.0.1:6379"
}

func newStores(t *testing.T) (*sessionredis.Store, *sessionredis.Store, func()) {
	t.Helper()
	addr := integrationAddress(t)
	prefix := fmt.Sprintf("due:test:session:ownership:%d", time.Now().UnixNano())
	first := sessionredis.NewStore(
		sessionredis.WithAddrs(addr),
		sessionredis.WithPrefix(prefix),
	)
	second := sessionredis.NewStore(
		sessionredis.WithAddrs(addr),
		sessionredis.WithPrefix(prefix),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := first.Ping(ctx); err != nil {
		t.Fatalf("ping first store: %v", err)
	}
	if err := second.Ping(ctx); err != nil {
		t.Fatalf("ping second store: %v", err)
	}

	client := goredis.NewClient(&goredis.Options{Addr: addr})
	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		iter := client.Scan(ctx, 0, prefix+":*", 0).Iterator()
		for iter.Next(ctx) {
			_ = client.Del(ctx, iter.Val()).Err()
		}
		_ = client.Close()
		_ = first.Close()
		_ = second.Close()
	}
	return first, second, cleanup
}

func TestRedisOwnershipStoreSharedGenerationAndCompareDelete(t *testing.T) {
	first, second, cleanup := newStores(t)
	defer cleanup()

	ctx := context.Background()
	const uid int64 = 1001

	token1, err := first.Acquire(ctx, uid)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	token2, err := second.Acquire(ctx, uid)
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if token1.UID != uid || token1.Generation == 0 {
		t.Fatalf("first token=%+v", token1)
	}
	if token2.UID != uid || token2.Generation <= token1.Generation {
		t.Fatalf("second token=%+v, want generation > %d", token2, token1.Generation)
	}

	current, ok, err := first.Current(ctx, uid)
	if err != nil {
		t.Fatalf("current through first store: %v", err)
	}
	if !ok || current != token2 {
		t.Fatalf("current=%+v found=%v, want %+v", current, ok, token2)
	}

	released, err := first.Release(ctx, uid, token1)
	if err != nil {
		t.Fatalf("release stale: %v", err)
	}
	if released {
		t.Fatal("stale token unexpectedly deleted current ownership")
	}
	current, ok, err = second.Current(ctx, uid)
	if err != nil || !ok || current != token2 {
		t.Fatalf("current after stale release=%+v found=%v err=%v", current, ok, err)
	}

	released, err = second.Release(ctx, uid, token2)
	if err != nil || !released {
		t.Fatalf("release current released=%v err=%v", released, err)
	}
	if _, ok, err := first.Current(ctx, uid); err != nil {
		t.Fatalf("current after release: %v", err)
	} else if ok {
		t.Fatal("current ownership still exists after release")
	}

	token3, err := first.Acquire(ctx, uid)
	if err != nil {
		t.Fatalf("third acquire: %v", err)
	}
	if token3.Generation <= token2.Generation {
		t.Fatalf("generation after release=%d, want > %d", token3.Generation, token2.Generation)
	}
}

func TestRedisOwnershipStoreConcurrentAcquireAcrossInstances(t *testing.T) {
	first, second, cleanup := newStores(t)
	defer cleanup()

	ctx := context.Background()
	const (
		uid   int64 = 2001
		total       = 64
	)

	var wg sync.WaitGroup
	wg.Add(total)
	tokens := make(chan session.Token, total)
	errs := make(chan error, total)

	for i := 0; i < total; i++ {
		store := first
		if i%2 == 1 {
			store = second
		}
		go func(store *sessionredis.Store) {
			defer wg.Done()
			token, err := store.Acquire(ctx, uid)
			if err != nil {
				errs <- err
				return
			}
			tokens <- token
		}(store)
	}
	wg.Wait()
	close(tokens)
	close(errs)

	for err := range errs {
		t.Fatalf("concurrent acquire: %v", err)
	}

	seen := make(map[uint64]struct{}, total)
	for token := range tokens {
		if token.UID != uid || token.Generation == 0 {
			t.Fatalf("invalid token %+v", token)
		}
		if _, exists := seen[token.Generation]; exists {
			t.Fatalf("duplicate generation %d", token.Generation)
		}
		seen[token.Generation] = struct{}{}
	}
	if len(seen) != total {
		t.Fatalf("unique generations=%d, want %d", len(seen), total)
	}

	current, ok, err := second.Current(ctx, uid)
	if err != nil || !ok {
		t.Fatalf("current found=%v err=%v", ok, err)
	}
	if current.Generation != total {
		t.Fatalf("current generation=%d, want %d", current.Generation, total)
	}
}
