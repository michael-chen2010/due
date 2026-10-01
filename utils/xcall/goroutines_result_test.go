package xcall_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dobyte/due/v2/utils/xcall"
)

func TestGoroutinesRunReportsDeadlineExceeded(t *testing.T) {
	release := make(chan struct{})
	g := xcall.NewGoroutines().Add(func() {
		<-release
	})

	err := g.Run(context.Background(), 20*time.Millisecond)
	close(release)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run error=%v, want context.DeadlineExceeded", err)
	}
}

func TestGoroutinesRunReturnsNilWhenAllFunctionsComplete(t *testing.T) {
	g := xcall.NewGoroutines().Add(func() {})

	if err := g.Run(context.Background(), time.Second); err != nil {
		t.Fatalf("Run error=%v, want nil", err)
	}
}
