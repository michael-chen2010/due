package link

import (
	"container/list"
	"sync"
)

type sourceHintKey struct {
	uid  int64
	name string
}
type sourceHint struct {
	key  sourceHintKey
	node string
}

// All entries are disposable routing hints. Owned source responsibilities must
// live separately and must never be removed by eviction or a watch event.
type sourceHints struct {
	mu       sync.Mutex
	capacity int
	revision uint64
	entries  map[sourceHintKey]*list.Element
	order    list.List
}

func newSourceHints(capacity int) *sourceHints {
	if capacity <= 0 {
		panic("source hint capacity must be positive")
	}
	return &sourceHints{capacity: capacity, entries: make(map[sourceHintKey]*list.Element)}
}
func (h *sourceHints) load(uid int64, name string) (string, bool, uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if e := h.entries[sourceHintKey{uid, name}]; e != nil {
		h.order.MoveToFront(e)
		return e.Value.(sourceHint).node, true, h.revision
	}
	return "", false, h.revision
}
func (h *sourceHints) putLocked(key sourceHintKey, node string) {
	if node == "" {
		return
	}
	if e := h.entries[key]; e != nil {
		e.Value = sourceHint{key, node}
		h.order.MoveToFront(e)
		return
	}
	if len(h.entries) == h.capacity {
		e := h.order.Back()
		delete(h.entries, e.Value.(sourceHint).key)
		h.order.Remove(e)
	}
	h.entries[key] = h.order.PushFront(sourceHint{key, node})
}
func (h *sourceHints) store(uid int64, name, node string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.revision++
	h.putLocked(sourceHintKey{uid, name}, node)
}
func (h *sourceHints) fill(uid int64, name, node string, revision uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if revision == h.revision {
		h.putLocked(sourceHintKey{uid, name}, node)
	}
}
func (h *sourceHints) invalidate(uid int64, name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.revision++
	key := sourceHintKey{uid, name}
	if e := h.entries[key]; e != nil {
		delete(h.entries, key)
		h.order.Remove(e)
	}
}
func (h *sourceHints) count() int { h.mu.Lock(); defer h.mu.Unlock(); return len(h.entries) }
