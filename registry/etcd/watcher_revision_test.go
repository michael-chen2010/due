package etcd

import (
	"context"
	"errors"
	"github.com/dobyte/due/v2/registry"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"sync"
	"testing"
	"time"
)

type gapSnapshotKV struct{ clientv3.KV }

func (*gapSnapshotKV) Get(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	// Revision 11 is committed after this snapshot, before Watch starts.
	return &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: 10}}, nil
}

type invalidSnapshotKV struct {
	clientv3.KV
	response *clientv3.GetResponse
	err      error
}

func (k *invalidSnapshotKV) Get(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return k.response, k.err
}
func TestWatcherRejectsInvalidSnapshotAndClosesOwnedWatcher(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response *clientv3.GetResponse
		err      error
	}{
		{"nil", nil, nil}, {"header", &clientv3.GetResponse{}, nil}, {"zero", &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{}}, nil}, {"negative", &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: -1}}, nil}, {"overflow", &clientv3.GetResponse{Header: &etcdserverpb.ResponseHeader{Revision: int64(1<<63 - 1)}}, nil}, {"backend", nil, errors.New("read canceled")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r := &Registry{ctx: ctx, opts: &options{client: &clientv3.Client{KV: &invalidSnapshotKV{response: tc.response, err: tc.err}}}}
			upstream := &gapRevisionWatcher{ch: make(chan clientv3.WatchResponse)}
			if _, e := newWatcherMgrWithWatcher(r, ctx, "game", upstream); e == nil {
				t.Fatal("invalid snapshot accepted")
			}
			if _, ok := <-upstream.ch; ok {
				t.Fatal("owned watcher not closed after snapshot failure")
			}
		})
	}
}

type gapRevisionWatcher struct {
	revision int64
	ch       chan clientv3.WatchResponse
	once     sync.Once
}

func (w *gapRevisionWatcher) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	w.revision = clientv3.OpGet(key, opts...).Rev()
	if w.revision == 11 {
		payload, _ := marshal(&registry.ServiceInstance{ID: "gap-node", Name: "game", Kind: "node", Endpoint: "tcp://127.0.0.1:1234"})
		w.ch <- clientv3.WatchResponse{Events: []*clientv3.Event{{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Key: []byte("/services/game/gap-node"), Value: []byte(payload), ModRevision: 11}}}}
	}
	return w.ch
}
func (*gapRevisionWatcher) RequestProgress(context.Context) error { return nil }
func (w *gapRevisionWatcher) Close() error                        { w.once.Do(func() { close(w.ch) }); return nil }

func TestWatcherReplaysRegistrationBetweenSnapshotAndWatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r := &Registry{ctx: ctx, opts: &options{client: &clientv3.Client{KV: &gapSnapshotKV{}}, namespace: "services"}}
	upstream := &gapRevisionWatcher{ch: make(chan clientv3.WatchResponse, 1)}
	m, e := newWatcherMgrWithWatcher(r, ctx, "game", upstream)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { m.cancel(); upstream.Close() }()
	if upstream.revision != 11 {
		t.Fatalf("list revision=10 but Watch starts at %d; registration in gap is lost", upstream.revision)
	}
	w := m.fork()
	for {
		services, e := w.Next()
		if e != nil {
			t.Fatal("gap registration not observed", e)
		}
		for _, s := range services {
			if s.ID == "gap-node" {
				return
			}
		}
	}
}
