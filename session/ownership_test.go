package session

import (
	"context"
	"sync"
	"testing"

	dueerrors "github.com/dobyte/due/v2/errors"
)

func TestMemoryOwnershipStoreAcquireAdvancesGeneration(t *testing.T) {
	store := NewMemoryOwnershipStore()
	ctx := context.Background()
	const uid int64 = 1001

	first, err := store.Acquire(ctx, uid)
	if err != nil {
		t.Fatalf("acquire first: %v", err)
	}
	if first.UID != uid || first.Generation == 0 {
		t.Fatalf("first token=%+v, want uid=%d and non-zero generation", first, uid)
	}

	second, err := store.Acquire(ctx, uid)
	if err != nil {
		t.Fatalf("acquire second: %v", err)
	}
	if second.UID != uid || second.Generation <= first.Generation {
		t.Fatalf("second token=%+v, want generation > %d", second, first.Generation)
	}

	current, ok, err := store.Current(ctx, uid)
	if err != nil {
		t.Fatalf("current: %v", err)
	}
	if !ok || current != second {
		t.Fatalf("current=%+v found=%v, want %+v", current, ok, second)
	}
}

func TestMemoryOwnershipStoreReleaseIsCompareAndDelete(t *testing.T) {
	store := NewMemoryOwnershipStore()
	ctx := context.Background()
	const uid int64 = 2001

	stale, err := store.Acquire(ctx, uid)
	if err != nil {
		t.Fatalf("acquire stale token: %v", err)
	}
	current, err := store.Acquire(ctx, uid)
	if err != nil {
		t.Fatalf("acquire current token: %v", err)
	}

	released, err := store.Release(ctx, uid, stale)
	if err != nil {
		t.Fatalf("release stale token: %v", err)
	}
	if released {
		t.Fatal("stale token unexpectedly released current ownership")
	}

	got, ok, err := store.Current(ctx, uid)
	if err != nil {
		t.Fatalf("current after stale release: %v", err)
	}
	if !ok || got != current {
		t.Fatalf("current after stale release=%+v found=%v, want %+v", got, ok, current)
	}

	released, err = store.Release(ctx, uid, current)
	if err != nil {
		t.Fatalf("release current token: %v", err)
	}
	if !released {
		t.Fatal("current token must release ownership")
	}
	if _, ok, err := store.Current(ctx, uid); err != nil {
		t.Fatalf("current after release: %v", err)
	} else if ok {
		t.Fatal("ownership still exists after current release")
	}
}

func TestMemoryOwnershipStoreGenerationSurvivesRelease(t *testing.T) {
	store := NewMemoryOwnershipStore()
	ctx := context.Background()
	const uid int64 = 3001

	first, err := store.Acquire(ctx, uid)
	if err != nil {
		t.Fatalf("acquire first: %v", err)
	}
	if released, err := store.Release(ctx, uid, first); err != nil || !released {
		t.Fatalf("release first released=%v err=%v", released, err)
	}
	second, err := store.Acquire(ctx, uid)
	if err != nil {
		t.Fatalf("acquire second: %v", err)
	}
	if second.Generation <= first.Generation {
		t.Fatalf("generation after release=%d, want > %d", second.Generation, first.Generation)
	}
}

func TestMemoryOwnershipStoreRejectsInvalidArguments(t *testing.T) {
	store := NewMemoryOwnershipStore()
	ctx := context.Background()

	if _, err := store.Acquire(ctx, 0); !dueerrors.Is(err, dueerrors.ErrInvalidArgument) {
		t.Fatalf("Acquire(0) err=%v, want ErrInvalidArgument", err)
	}
	if _, _, err := store.Current(ctx, 0); !dueerrors.Is(err, dueerrors.ErrInvalidArgument) {
		t.Fatalf("Current(0) err=%v, want ErrInvalidArgument", err)
	}
	if released, err := store.Release(ctx, 1001, Token{}); !dueerrors.Is(err, dueerrors.ErrInvalidArgument) || released {
		t.Fatalf("Release(zero token) released=%v err=%v, want invalid argument", released, err)
	}
	if released, err := store.Release(ctx, 1001, Token{UID: 2002, Generation: 1}); !dueerrors.Is(err, dueerrors.ErrInvalidArgument) || released {
		t.Fatalf("Release(mismatched uid) released=%v err=%v, want invalid argument", released, err)
	}
}

func TestMemoryOwnershipStoreConcurrentAcquireProducesUniqueGenerations(t *testing.T) {
	store := NewMemoryOwnershipStore()
	ctx := context.Background()
	const (
		uid   int64 = 3501
		total       = 64
	)

	var wg sync.WaitGroup
	wg.Add(total)
	tokens := make(chan Token, total)
	errs := make(chan error, total)
	for range total {
		go func() {
			defer wg.Done()
			token, err := store.Acquire(ctx, uid)
			if err != nil {
				errs <- err
				return
			}
			tokens <- token
		}()
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
			t.Fatalf("invalid concurrent token: %+v", token)
		}
		if _, exists := seen[token.Generation]; exists {
			t.Fatalf("duplicate generation: %d", token.Generation)
		}
		seen[token.Generation] = struct{}{}
	}
	if len(seen) != total {
		t.Fatalf("unique generations=%d, want %d", len(seen), total)
	}

	current, ok, err := store.Current(ctx, uid)
	if err != nil || !ok {
		t.Fatalf("current after concurrent acquire found=%v err=%v", ok, err)
	}
	if current.Generation != total {
		t.Fatalf("current generation=%d, want %d", current.Generation, total)
	}
}

func TestMemoryOwnershipStoreRespectsContextCancellation(t *testing.T) {
	store := NewMemoryOwnershipStore()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := store.Acquire(ctx, 4001); err != context.Canceled {
		t.Fatalf("Acquire canceled err=%v, want context.Canceled", err)
	}
	if _, _, err := store.Current(ctx, 4001); err != context.Canceled {
		t.Fatalf("Current canceled err=%v, want context.Canceled", err)
	}
	if released, err := store.Release(ctx, 4001, Token{UID: 4001, Generation: 1}); err != context.Canceled || released {
		t.Fatalf("Release canceled released=%v err=%v, want context.Canceled", released, err)
	}
}
