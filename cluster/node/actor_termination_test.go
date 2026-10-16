package node

import (
	"errors"
	"sync"
	"testing"
	"time"

	dueerrors "github.com/dobyte/due/v2/errors"
)

type terminationProcessor struct {
	BaseProcessor
	entered, allow chan struct{}
}

func (p *terminationProcessor) Destroy() { close(p.entered); <-p.allow }

func TestActorTerminationWaitsForProcessorAndReleaseHook(t *testing.T) {
	n := NewNode()
	p := &terminationProcessor{entered: make(chan struct{}), allow: make(chan struct{})}
	hookEntered, hookAllow := make(chan struct{}), make(chan struct{})
	var processorOnce, hookOnce sync.Once
	allowProcessor := func() { processorOnce.Do(func() { close(p.allow) }) }
	allowHook := func() { hookOnce.Do(func() { close(hookAllow) }) }
	t.Cleanup(func() { allowProcessor(); allowHook() })
	opts := []ActorOption{WithActorKind("player"), WithActorID("terminal-proof"), WithActorNonWait(), WithActorNonDispatch(), WithActorReleaseHook(func(*Actor) { close(hookEntered); <-hookAllow })}
	a, conflict, err := n.Proxy().SpawnObserved(func(*Actor, ...any) Processor { return p }, opts...)
	if err != nil || conflict != nil || a.Termination() == nil || a.Termination().Complete() {
		t.Fatal("invalid initial termination observation", err)
	}
	other, conflict, err := n.Proxy().SpawnObserved(func(*Actor, ...any) Processor { return new(BaseProcessor) }, opts...)
	if other != nil || !errors.Is(err, dueerrors.ErrActorExists) || conflict != a.Termination() {
		t.Fatal("conflict not captured at scheduler decision", err)
	}
	done := make(chan struct{})
	result := make(chan bool, 1)
	go func() { result <- a.Destroy(); close(done) }()
	t.Cleanup(func() { allowProcessor(); allowHook(); <-done })
	select {
	case <-p.entered:
	case <-time.After(time.Second):
		t.Fatal("Destroy not entered")
	}
	if _, exists := n.Proxy().Actor("player", "terminal-proof"); exists || conflict.Complete() {
		t.Fatal("scheduler removal reported terminal completion")
	}
	allowProcessor()
	select {
	case <-hookEntered:
	case <-time.After(time.Second):
		t.Fatal("hook not entered")
	}
	if conflict.Complete() {
		t.Fatal("hook still running but proof complete")
	}
	allowHook()
	select {
	case <-done:
		if !<-result {
			t.Fatal("Destroy false")
		}
	case <-time.After(time.Second):
		t.Fatal("Destroy did not join")
	}
	if !conflict.Complete() || a.Destroy() {
		t.Fatal("completion/duplicate Destroy invalid")
	}
	var empty *ActorTermination
	if empty.Complete() || new(ActorTermination).Complete() || (*Actor)(nil).Termination() != nil {
		t.Fatal("missing witness grants completion")
	}
}
