package gate

import (
	"context"
	"errors"
	"testing"

	dueerrors "github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/locate"
	"github.com/dobyte/due/v2/registry"
	"github.com/dobyte/due/v2/session"
)

type controlClientTestLocator struct {
	locate.Locator
	gid   string
	err   error
	calls int
}

func (l *controlClientTestLocator) LocateGate(
	_ context.Context,
	uid int64,
) (string, error) {
	l.calls++
	if l.err != nil {
		return "", l.err
	}
	return l.gid, nil
}

type controlClientTestRegistry struct {
	registry.Registry
	services []*registry.ServiceInstance
	err      error
	calls    int
	name     string
}

func (r *controlClientTestRegistry) Services(
	_ context.Context,
	name string,
) ([]*registry.ServiceInstance, error) {
	r.calls++
	r.name = name
	if r.err != nil {
		return nil, r.err
	}
	return r.services, nil
}

type controlClientTestGate struct {
	kind   session.Kind
	target int64
	token  session.Token
	force  bool
	calls  int
	err    error
}

func (g *controlClientTestGate) DisconnectCurrent(
	_ context.Context,
	kind session.Kind,
	target int64,
	token session.Token,
	force bool,
) error {
	g.calls++
	g.kind = kind
	g.target = target
	g.token = token
	g.force = force
	return g.err
}

func TestControlClientDisconnectCurrentUsesLocatedExactGateAndToken(t *testing.T) {
	locator := &controlClientTestLocator{gid: "gate-2"}
	registrySource := &controlClientTestRegistry{
		services: []*registry.ServiceInstance{
			{ID: "gate-1", Name: "gate", Endpoint: "tcp://127.0.0.1:7101"},
			{ID: "gate-2", Name: "gate", Endpoint: "tcp://127.0.0.1:7102"},
		},
	}
	gateClient := &controlClientTestGate{}
	var builtAddress string

	client, err := newControlClient(
		ControlClientConfig{
			ID:       "admin-control-1",
			Locator:  locator,
			Registry: registrySource,
		},
		func(address string) (controlGateClient, error) {
			builtAddress = address
			return gateClient, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	token := session.Token{UID: 9001, Generation: 17}
	if err := client.DisconnectCurrent(
		context.Background(),
		token.UID,
		token,
		true,
	); err != nil {
		t.Fatalf("DisconnectCurrent: %v", err)
	}
	if locator.calls != 1 || registrySource.calls != 1 {
		t.Fatalf(
			"locator/registry calls=%d/%d want=1/1",
			locator.calls,
			registrySource.calls,
		)
	}
	if registrySource.name != "gate" {
		t.Fatalf("registry service=%q want gate", registrySource.name)
	}
	if builtAddress != "127.0.0.1:7102" {
		t.Fatalf("built address=%q want 127.0.0.1:7102", builtAddress)
	}
	if gateClient.calls != 1 ||
		gateClient.kind != session.User ||
		gateClient.target != token.UID ||
		gateClient.token != token ||
		!gateClient.force {
		t.Fatalf("gate call=%+v", gateClient)
	}
}

func TestControlClientDisconnectCurrentRejectsStaleOrMismatchedTokenBeforeLookup(t *testing.T) {
	locator := &controlClientTestLocator{gid: "gate-1"}
	registrySource := &controlClientTestRegistry{}
	client, err := newControlClient(
		ControlClientConfig{
			ID:       "admin-control-2",
			Locator:  locator,
			Registry: registrySource,
		},
		func(string) (controlGateClient, error) {
			t.Fatal("builder must not be called")
			return nil, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name  string
		uid   int64
		token session.Token
	}{
		{name: "zero", uid: 9001},
		{
			name:  "wrong uid",
			uid:   9001,
			token: session.Token{UID: 9002, Generation: 1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := client.DisconnectCurrent(
				context.Background(),
				tc.uid,
				tc.token,
				true,
			)
			if !errors.Is(err, dueerrors.ErrStaleSession) {
				t.Fatalf("error=%v want=%v", err, dueerrors.ErrStaleSession)
			}
		})
	}
	if locator.calls != 0 || registrySource.calls != 0 {
		t.Fatalf(
			"invalid token touched locator/registry=%d/%d",
			locator.calls,
			registrySource.calls,
		)
	}
}

func TestControlClientDisconnectCurrentFailsClosedWhenLocatedGateIsMissing(t *testing.T) {
	locator := &controlClientTestLocator{gid: "gate-missing"}
	registrySource := &controlClientTestRegistry{
		services: []*registry.ServiceInstance{
			{ID: "gate-other", Name: "gate", Endpoint: "tcp://127.0.0.1:7101"},
		},
	}
	client, err := newControlClient(
		ControlClientConfig{
			ID:       "admin-control-3",
			Locator:  locator,
			Registry: registrySource,
		},
		func(string) (controlGateClient, error) {
			t.Fatal("builder must not be called")
			return nil, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	token := session.Token{UID: 9003, Generation: 2}
	err = client.DisconnectCurrent(
		context.Background(),
		token.UID,
		token,
		true,
	)
	if !errors.Is(err, dueerrors.ErrNotFoundEndpoint) {
		t.Fatalf("error=%v want=%v", err, dueerrors.ErrNotFoundEndpoint)
	}
}
