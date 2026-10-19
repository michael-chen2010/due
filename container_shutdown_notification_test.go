package due

import (
	"testing"
	"time"
)

type shutdownNotification interface {
	ShutdownStarted() <-chan struct{}
	ShutdownStartedAt() time.Time
}

func TestContainerShutdownNotificationStartsOnceBeforeCleanup(t *testing.T) {
	c := NewContainer()
	observed, ok := any(c).(shutdownNotification)
	if !ok {
		t.Fatal("container does not expose shutdown start for process deadline")
	}
	signal := observed.ShutdownStarted()
	select {
	case <-signal:
		t.Fatal("shutdown reported before request")
	default:
	}
	if !observed.ShutdownStartedAt().IsZero() {
		t.Fatal("shutdown timestamp set before request")
	}
	before := time.Now()
	c.Shutdown()
	select {
	case <-signal:
	default:
		t.Fatal("shutdown request did not notify process deadline")
	}
	started := observed.ShutdownStartedAt()
	if started.Before(before) || started.After(time.Now()) {
		t.Fatalf("invalid shutdown start: %v", started)
	}
	c.Shutdown()
	if !started.Equal(observed.ShutdownStartedAt()) {
		t.Fatal("repeat shutdown reset deadline")
	}
}

func TestContainerOnceShutdownNotifiesProcess(t *testing.T) {
	c := NewContainer()
	observed, ok := any(c).(shutdownNotification)
	if !ok {
		t.Fatal("container does not expose shutdown start for process deadline")
	}
	if err := c.finishShutdown(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-observed.ShutdownStarted():
	default:
		t.Fatal("once shutdown did not notify process deadline")
	}
}
