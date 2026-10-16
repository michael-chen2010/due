package gate

import (
	"context"
	"testing"
)

func TestSourceHintOptionReachesGateNodeLinker(t *testing.T) {
	opts := defaultOptions()
	WithSourceHintCapacity(2)(opts)
	g := &Gate{opts: opts, ctx: context.Background()}
	g.proxy = newProxy(g)
	capacity, count := g.SourceHintCounts()
	if capacity != 2 || count != 0 {
		t.Fatal("source hint option not propagated")
	}
	defer func() {
		if recover() == nil {
			t.Error("negative hint capacity accepted")
		}
	}()
	WithSourceHintCapacity(-1)(opts)
}
