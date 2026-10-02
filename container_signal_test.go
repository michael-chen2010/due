package due

import (
	"testing"
	"time"
)

func TestNewSignalChannelIsBuffered(t *testing.T) {
	sig := newSignalChannel()
	if got := cap(sig); got != 1 {
		t.Fatalf("signal channel capacity=%d, want 1", got)
	}
}

func TestContainerShutdownUnblocksSignalWait(t *testing.T) {
	container := NewContainer()
	container.Shutdown()
	container.Shutdown()

	done := make(chan struct{})
	go func() {
		container.doWaitSystemSignal()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("programmatic shutdown did not unblock signal wait")
	}
}
