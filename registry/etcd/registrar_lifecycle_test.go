package etcd

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dobyte/due/v2/registry"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestRegistrarDispatchHeartbeatReturnsWhenWorkerAlreadyStopped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	r := &registrar{ctx: ctx, chHeartbeat: make(chan heartbeat)}
	cancel() // simulate the already exited registrar heartbeat loop

	done := make(chan error, 1)
	go func() { done <- r.dispatchHeartbeat(context.Background(), heartbeat{key: "node"}) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("dispatch after registrar cancel: got %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("dispatch blocked without heartbeat receiver after registrar cancellation")
	}
}

func TestRegistrarDispatchHeartbeatHonorsCallerDeadlineWithoutReceiver(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &registrar{ctx: ctx, chHeartbeat: make(chan heartbeat)}
	callCtx, cancelCall := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelCall()

	err := r.dispatchHeartbeat(callCtx, heartbeat{key: "node"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("dispatch with no receiver: got %v, want deadline exceeded", err)
	}
}

func TestRegistrarDispatchHeartbeatDeliversWhenActive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &registrar{ctx: ctx, chHeartbeat: make(chan heartbeat)}
	received := make(chan heartbeat, 1)
	go func() { received <- <-r.chHeartbeat }()

	if err := r.dispatchHeartbeat(context.Background(), heartbeat{key: "updated-node"}); err != nil {
		t.Fatalf("live heartbeat dispatch: %v", err)
	}
	select {
	case msg := <-received:
		if msg.key != "updated-node" {
			t.Fatalf("received key %q", msg.key)
		}
	case <-time.After(time.Second):
		t.Fatal("active heartbeat update not delivered")
	}
}

func TestRegistrarDispatchConcurrentCancellationCannotHang(t *testing.T) {
	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		r := &registrar{ctx: ctx, chHeartbeat: make(chan heartbeat)}
		done := make(chan error, 1)
		go func() { done <- r.dispatchHeartbeat(context.Background(), heartbeat{}) }()
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("iteration %d: got %v, want canceled", i, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("iteration %d: dispatch blocked during concurrent shutdown", i)
		}
	}
}

type stalledPutKV struct {
	clientv3.KV
	started chan struct{}
	release chan struct{}
}

func (k *stalledPutKV) Put(context.Context, string, string, ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	close(k.started)
	<-k.release
	return &clientv3.PutResponse{}, nil
}

func (k *stalledPutKV) Delete(context.Context, string, ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	return &clientv3.DeleteResponse{}, nil
}

type registrarLifecycleLease struct{ clientv3.Lease }

func (l registrarLifecycleLease) Grant(context.Context, int64) (*clientv3.LeaseGrantResponse, error) {
	return &clientv3.LeaseGrantResponse{ID: 1}, nil
}

func (l registrarLifecycleLease) Close() error { return nil }

func TestRegistrarConcurrentDeregisterWhileRegisterPutInFlightDoesNotPanicOrHang(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()

	kv := &stalledPutKV{started: make(chan struct{}), release: make(chan struct{})}
	r := &registrar{
		registry:    &Registry{opts: &options{namespace: "isolated", retryInterval: time.Second}},
		ctx:         parent,
		cancel:      cancel,
		kv:          kv,
		lease:       registrarLifecycleLease{},
		chHeartbeat: make(chan heartbeat), // no worker, as after shutdown
	}
	ins := &registry.ServiceInstance{ID: "isolated-node", Name: "node"}
	finished := make(chan error, 1)
	go func() { finished <- r.register(context.Background(), ins) }()

	select {
	case <-kv.started:
	case <-time.After(time.Second):
		t.Fatal("register did not begin put")
	}

	if err := r.deregister(context.Background(), ins); err != nil {
		close(kv.release)
		t.Fatalf("concurrent deregister failed: %v", err)
	}
	close(kv.release)

	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("register finished with %v; want cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("register blocked after registrar deregistration")
	}
}
