package node

import "testing"

func TestSourceHintOptionReachesBothNodeLinkers(t *testing.T) {
	opts := defaultOptions()
	WithSourceHintCapacity(2)(opts)
	p := newProxy(&Node{opts: opts})
	capacity, nodes, gates := p.SourceHintCounts()
	if capacity != 2 || nodes != 0 || gates != 0 {
		t.Fatal("source hint option not propagated")
	}
	defer func() {
		if recover() == nil {
			t.Error("negative hint capacity accepted")
		}
	}()
	WithSourceHintCapacity(-1)(opts)
}
