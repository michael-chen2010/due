package node

import (
	"context"
	"errors"
	"sync"
	"testing"

	linknode "github.com/dobyte/due/v2/internal/transporter/node"
	"github.com/dobyte/due/v2/registry"
)

type metadataUpdateRegistry struct {
	mu        sync.Mutex
	registers []*registry.ServiceInstance
	err       error
}

func (r *metadataUpdateRegistry) Name() string { return "metadata-update-test" }

func (r *metadataUpdateRegistry) Register(
	_ context.Context,
	ins *registry.ServiceInstance,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	clone := *ins
	clone.Metadata = cloneStringMap(ins.Metadata)
	r.registers = append(r.registers, &clone)
	return r.err
}

func (r *metadataUpdateRegistry) Deregister(
	context.Context,
	*registry.ServiceInstance,
) error {
	return nil
}

func (r *metadataUpdateRegistry) Watch(
	context.Context,
	string,
) (registry.Watcher, error) {
	return nil, errors.New("unused")
}

func (r *metadataUpdateRegistry) Services(
	context.Context,
	string,
) ([]*registry.ServiceInstance, error) {
	return nil, nil
}

func (r *metadataUpdateRegistry) lastRegister() *registry.ServiceInstance {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.registers) == 0 {
		return nil
	}
	clone := *r.registers[len(r.registers)-1]
	clone.Metadata = cloneStringMap(clone.Metadata)
	return &clone
}

func TestProxyUpdateMetadataMergesAndReregistersExistingInstance(t *testing.T) {
	reg := &metadataUpdateRegistry{}
	n := NewNode(
		WithRegistry(reg),
		WithMetadata(map[string]string{
			"build": "m2-build",
		}),
	)
	n.instances = []*registry.ServiceInstance{{
		ID:       "game-1",
		Name:     "node",
		Kind:     "node",
		State:    "work",
		Metadata: map[string]string{"build": "m2-build"},
	}}

	patch := map[string]string{
		"task_version":     "7",
		"last_apply_error": "",
	}
	if err := n.Proxy().UpdateMetadata(patch); err != nil {
		t.Fatalf("UpdateMetadata: %v", err)
	}
	patch["task_version"] = "999"

	got := reg.lastRegister()
	if got == nil {
		t.Fatal("registry did not receive refreshed service instance")
	}
	if got.Metadata["build"] != "m2-build" ||
		got.Metadata["task_version"] != "7" ||
		got.Metadata["last_apply_error"] != "" {
		t.Fatalf("registered metadata=%v", got.Metadata)
	}
	if n.opts.metadata["task_version"] != "7" {
		t.Fatalf("node metadata aliased caller patch: %v", n.opts.metadata)
	}
	if n.instances[0].Metadata["task_version"] != "7" {
		t.Fatalf("instance metadata not refreshed: %v", n.instances[0].Metadata)
	}
}

func TestProxyUpdateMetadataPropagatesRegistryFailure(t *testing.T) {
	wantErr := errors.New("registry unavailable")
	reg := &metadataUpdateRegistry{err: wantErr}
	n := NewNode(WithRegistry(reg))
	n.instances = []*registry.ServiceInstance{{
		ID:       "game-1",
		Name:     "node",
		Kind:     "node",
		State:    "work",
		Metadata: map[string]string{},
	}}

	err := n.Proxy().UpdateMetadata(map[string]string{"task_version": "8"})
	if !errors.Is(err, wantErr) {
		t.Fatalf("UpdateMetadata error=%v want=%v", err, wantErr)
	}
}

func cloneStringMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

// Status reporters can publish before Node.Start finishes registering instances.
func TestRegisterServiceInstancesConcurrentStartupMetadata(t *testing.T) {
	reg := &metadataUpdateRegistry{}
	n := NewNode(WithRegistry(reg), WithMetadata(map[string]string{"build": "m6"}))
	defer n.cancel()
	var err error
	n.linker, err = linknode.NewServer(&provider{node: n}, &linknode.ServerOptions{Addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-start
		for i := 0; i < 200; i++ {
			if err := n.updateMetadata(map[string]string{"status": "pending"}); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	close(start)
	for i := 0; i < 20; i++ {
		n.registerServiceInstances()
	}
	<-done
	if err := n.updateMetadata(map[string]string{"status": "ready"}); err != nil {
		t.Fatal(err)
	}
	got := reg.lastRegister()
	if got == nil || got.Metadata["build"] != "m6" || got.Metadata["status"] != "ready" {
		t.Fatalf("startup metadata lost: %#v", got)
	}
}
