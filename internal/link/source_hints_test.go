package link

import (
	"sync"
	"testing"
)

func TestSourceHintsBoundedLRUAndLateFill(t *testing.T) {
	h := newSourceHints(2)
	h.store(1, "game", "a")
	h.store(2, "game", "b")
	if v, ok, _ := h.load(1, "game"); !ok || v != "a" {
		t.Fatal("cache hit missing")
	}
	h.store(3, "chat", "c")
	if _, ok, _ := h.load(2, "game"); ok {
		t.Fatal("least recently used hint retained")
	}
	_, _, revision := h.load(99, "game")
	h.invalidate(99, "game")
	h.fill(99, "game", "stale", revision)
	if _, ok, _ := h.load(99, "game"); ok {
		t.Fatal("query predating invalidation refilled stale hint")
	}
	for i := int64(1); i < 1000; i++ {
		h.store(i, "game", "n")
	}
	if n := h.count(); n != 2 {
		t.Fatal("unbounded hint count", n)
	}
	h.store(999, "game", "updated")
	if n := h.count(); n != 2 {
		t.Fatal("replacement inflated count")
	}
}

func TestSourceHintsConcurrentBoundAndInvalidation(t *testing.T) {
	h := newSourceHints(8)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := int64(0); i < 1000; i++ {
				id := i + int64(w)*1000
				_, _, r := h.load(id, "game")
				h.fill(id, "game", "remote", r)
				h.store(id, "gate", "remote")
				h.invalidate(id, "game")
				if n := h.count(); n > 8 {
					t.Error("capacity exceeded", n)
				}
			}
		}(w)
	}
	wg.Wait()
}
