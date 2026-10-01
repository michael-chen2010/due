package due

import (
	"testing"
	"time"

	"github.com/dobyte/due/v2/component"
	"github.com/dobyte/due/v2/etc"
)

type blockingDestroyComponent struct {
	component.Base
	release chan struct{}
}

func (c *blockingDestroyComponent) Destroy() {
	<-c.release
}

func TestContainerDestroyUsesConfiguredShutdownMaxWaitTime(t *testing.T) {
	const key = "etc.shutdownMaxWaitTime"
	original := etc.Get(key).String()
	t.Cleanup(func() {
		_ = etc.Set(key, original)
	})
	if err := etc.Set(key, 25*time.Millisecond); err != nil {
		t.Fatalf("set shutdown timeout: %v", err)
	}

	comp := &blockingDestroyComponent{release: make(chan struct{})}
	container := NewContainer()
	container.Add(comp)

	start := time.Now()
	container.doDestroyComponents()
	elapsed := time.Since(start)
	close(comp.release)

	if elapsed > 500*time.Millisecond {
		t.Fatalf("destroy elapsed=%v, want configured shutdown deadline near 25ms", elapsed)
	}
}
