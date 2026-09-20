package lock_test

import (
	"context"
	"testing"
	"time"

	"github.com/dobyte/due/v2/lock"
)

type testMaker struct {
	name   string
	locker *testLocker
}

func (m *testMaker) Make(name string) lock.Locker {
	m.name = name
	return m.locker
}

func (*testMaker) Close() error {
	return nil
}

type testLocker struct {
	acquired bool
}

func (l *testLocker) Acquire(context.Context) error {
	l.acquired = true
	return nil
}

func (l *testLocker) TryAcquire(context.Context, ...time.Duration) error {
	l.acquired = true
	return nil
}

func (l *testLocker) Release(context.Context) error {
	l.acquired = false
	return nil
}

func TestMake(t *testing.T) {
	fakeLocker := &testLocker{}
	maker := &testMaker{locker: fakeLocker}
	lock.SetMaker(maker)

	locker := lock.Make("lockName")
	if locker == nil {
		t.Fatal("lock.Make returned nil after registering maker")
	}
	if maker.name != "lockName" {
		t.Fatalf("maker received lock name %q, want %q", maker.name, "lockName")
	}

	ctx := context.Background()
	if err := locker.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	if !fakeLocker.acquired {
		t.Fatal("locker was not marked acquired")
	}

	if err := locker.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if fakeLocker.acquired {
		t.Fatal("locker remained acquired after release")
	}
}
