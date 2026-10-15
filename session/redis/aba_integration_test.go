package redis_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	sessionredis "github.com/dobyte/due/session/redis/v2"
	goredis "github.com/redis/go-redis/v9"
)

// This is a direct Store regression for AUDIT-007. Existing full two-Gate
// socket-close RED fixtures remain unchanged in WebGameServer.
func TestRedisOwnershipWholeHashLossCannotReuseFencingGeneration(t *testing.T) {
	if os.Getenv("DUE_SESSION_REDIS_ABA_ACCEPTANCE") != "1" {
		t.Skip("explicit isolated ABA regression required")
	}
	if os.Getenv("DUE_REDIS") != "127.0.0.1:26379" {
		t.Fatal("refusing non-D050 Redis")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := goredis.NewClient(&goredis.Options{Addr: os.Getenv("DUE_REDIS")})
	t.Cleanup(func() { client.Close() })
	prefix := fmt.Sprintf("due:test:session:aba:%d", time.Now().UnixNano())
	const uid int64 = 7001
	key := fmt.Sprintf("%s:%d", prefix, uid)
	if n, e := client.Exists(ctx, key).Result(); e != nil || n != 0 {
		t.Fatal("owned key collision", n, e)
	}
	t.Cleanup(func() {
		clean, done := context.WithTimeout(context.Background(), 3*time.Second)
		defer done()
		if e := client.Del(clean, key).Err(); e != nil {
			t.Error(e)
		}
		if n, e := client.Exists(clean, key).Result(); e != nil || n != 0 {
			t.Error("owned key not zero", n, e)
		}
	})
	first := sessionredis.NewStore(sessionredis.WithClient(client), sessionredis.WithPrefix(prefix))
	second := sessionredis.NewStore(sessionredis.WithClient(client), sessionredis.WithPrefix(prefix))
	old, e := first.Acquire(ctx, uid)
	if e != nil {
		t.Fatal(e)
	}
	// Delete only this test's previously absent hash to emulate exact-key loss.
	if n, e := client.Del(ctx, key).Result(); e != nil || n != 1 {
		t.Fatal("owned hash loss not exercised", n, e)
	}
	current, e := second.Acquire(ctx, uid)
	if e != nil {
		t.Fatal(e)
	}
	if current.Generation <= old.Generation {
		t.Errorf("lost hash reused fencing generation old=%d new=%d", old.Generation, current.Generation)
	}
	released, e := first.Release(ctx, uid, old)
	if e != nil {
		t.Fatal(e)
	}
	if released {
		t.Error("stale release erased replacement ownership after hash loss")
	}
	actual, found, e := second.Current(ctx, uid)
	if e != nil || !found || actual != current {
		t.Errorf("replacement lost after stale release found=%v actual=%+v want=%+v err=%v", found, actual, current, e)
	}
}
