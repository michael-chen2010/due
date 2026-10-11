package node

import (
	"context"
	"github.com/dobyte/due/v2/cluster"
	"reflect"
	"testing"
	"time"
)

func TestRouterGlobalMiddlewareCoversLateRoutesAndDeniesContinuation(t *testing.T) {
	n := NewNode(WithID("global-middleware-test"), WithName("game"))
	order := []string{}
	n.router.SetRequestMiddleware(func(m *Middleware, ctx Context) {
		order = append(order, "global")
		if ctx.Route() != 52 {
			m.Next(ctx)
		}
	})
	n.router.AddRouteHandler(51, func(Context) { order = append(order, "route") }, RouteOptions{Middlewares: []MiddlewareHandler{func(m *Middleware, ctx Context) { order = append(order, "local"); m.Next(ctx) }}})
	n.router.AddRouteHandler(52, func(Context) { t.Fatal("denied route executed") })
	n.router.handle(&request{node: n, ctx: context.Background(), message: &cluster.Message{Route: 51}})
	n.router.handle(&request{node: n, ctx: context.Background(), message: &cluster.Message{Route: 52}})
	if !reflect.DeepEqual(order, []string{"global", "local", "route", "global"}) {
		t.Fatal("middleware order/coverage incorrect", order)
	}
}

func TestRouterGlobalMiddlewareTaskDoesNotBlockOtherPlayer(t *testing.T) {
	n := NewNode(WithID("global-task-test"), WithName("game"))
	blocked := make(chan struct{})
	release := make(chan struct{})
	completed := make(chan int32, 2)
	n.router.SetRequestMiddleware(func(m *Middleware, ctx Context) {
		ctx.Task(func(task Context) {
			if task.Route() == 61 {
				close(blocked)
				<-release
			}
			m.Next(task)
		})
	})
	for _, route := range []int32{61, 62} {
		n.router.AddRouteHandler(route, func(ctx Context) { completed <- ctx.Route() })
	}
	n.router.handle(&request{node: n, ctx: context.Background(), message: &cluster.Message{Route: 61}})
	<-blocked
	n.router.handle(&request{node: n, ctx: context.Background(), message: &cluster.Message{Route: 62}})
	select {
	case route := <-completed:
		if route != 62 {
			t.Fatal("unexpected player completed")
		}
	case <-time.After(time.Second):
		t.Fatal("global authorization blocked router")
	}
	close(release)
	select {
	case route := <-completed:
		if route != 61 {
			t.Fatal("accepted task disappeared")
		}
	case <-time.After(time.Second):
		t.Fatal("middleware Task lost continuation")
	}
	n.wg.Wait()
}

func TestRouterGlobalMiddlewareCoversDefaultAndCopiesConfiguration(t *testing.T) {
	n := NewNode(WithID("global-default-test"), WithName("game"))
	calls := 0
	handlers := []MiddlewareHandler{func(m *Middleware, ctx Context) { calls++; m.Next(ctx) }}
	n.router.SetRequestMiddleware(handlers...)
	handlers[0] = func(*Middleware, Context) { t.Fatal("caller mutated global middleware") }
	n.router.SetDefaultRouteHandler(func(Context) { calls++ })
	n.router.handle(&request{node: n, ctx: context.Background(), message: &cluster.Message{Route: 71}})
	if calls != 2 {
		t.Fatal("default handler bypassed global guard")
	}
	n.router.SetRequestMiddleware(nil)
	n.router.handle(&request{node: n, ctx: context.Background(), message: &cluster.Message{Route: 72}})
	if calls != 4 {
		t.Fatal("invalid configuration silently removed guards")
	}
}
