package node

import "testing"

func TestRouterCheckRouteAuthorized(t *testing.T) {
	n := NewNode(
		WithID("route-options-test"),
		WithName("game"),
	)
	const authorizedRoute int32 = 99101
	const publicRoute int32 = 99102

	n.Proxy().AddRouteHandler(
		authorizedRoute,
		func(Context) {},
		RouteOptions{Stateful: true, Authorized: true},
	)
	n.Proxy().AddRouteHandler(
		publicRoute,
		func(Context) {},
		RouteOptions{Stateful: true},
	)

	authorized, exists := n.Proxy().Router().CheckRouteAuthorized(
		authorizedRoute,
	)
	if !exists || !authorized {
		t.Fatalf(
			"authorized route exists/authorized=%v/%v want true/true",
			exists,
			authorized,
		)
	}

	authorized, exists = n.Proxy().Router().CheckRouteAuthorized(publicRoute)
	if !exists || authorized {
		t.Fatalf(
			"public route exists/authorized=%v/%v want true/false",
			exists,
			authorized,
		)
	}

	authorized, exists = n.Proxy().Router().CheckRouteAuthorized(99999)
	if exists || authorized {
		t.Fatalf(
			"missing route exists/authorized=%v/%v want false/false",
			exists,
			authorized,
		)
	}
}
