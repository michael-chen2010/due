package node

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/dobyte/due/v2/cluster"
	"github.com/dobyte/due/v2/session"
	"github.com/dobyte/due/v2/utils/xcall"
)

type Creator func(actor *Actor, args ...any) Processor

const (
	unstart   int32 = iota // 未启动
	started                // 已启动
	destroyed              // 已销毁
)

type Actor struct {
	opts                *actorOptions                  // 配置项
	scheduler           *Scheduler                     // 调度器
	state               atomic.Int32                   // 状态
	routes              map[int32]RouteHandler         // 路由处理器
	events              map[cluster.Event]EventHandler // 事件处理器
	defaultRouteHandler RouteHandler                   // 默认路由处理器
	processor           Processor                      // 处理器
	rw                  sync.RWMutex                   // 锁
	mailbox             chan Context                   // 邮箱
	fnChan              chan func()                    // 调用函数
	binds               sync.Map                       // 绑定的用户
	lifecycleMu         sync.Mutex                     // 生命周期定时器锁
	idleTimer           *time.Timer                    // Idle 自动释放定时器
	idleGeneration      uint64                         // 防止旧定时器释放新一代 Actor 状态
}

// ID 获取Actor的ID
func (a *Actor) ID() string {
	return a.opts.id
}

// PID 获取Actor的唯一识别ID
func (a *Actor) PID() string {
	return a.Kind() + "/" + a.ID()
}

// Kind 获取Actor类型
func (a *Actor) Kind() string {
	return a.opts.kind
}

// Active 标记 Actor 已重新活跃，并取消当前 Idle 自动释放计划。
func (a *Actor) Active() {
	if a == nil {
		return
	}

	a.lifecycleMu.Lock()
	defer a.lifecycleMu.Unlock()
	if a.state.Load() != started {
		return
	}

	a.idleGeneration++
	if a.idleTimer != nil {
		a.idleTimer.Stop()
		a.idleTimer = nil
	}
}

// Touch 等价于 Active，用于处理任意能够证明 Actor 仍活跃的事件。
func (a *Actor) Touch() {
	a.Active()
}

// Idle 安排 Actor 在配置的 idleTimeout 后自动释放。
func (a *Actor) Idle() {
	if a == nil || a.opts == nil || a.opts.idleTimeout <= 0 {
		return
	}

	a.lifecycleMu.Lock()
	if a.state.Load() != started {
		a.lifecycleMu.Unlock()
		return
	}
	if a.idleTimer != nil {
		a.idleTimer.Stop()
	}
	a.idleGeneration++
	generation := a.idleGeneration
	a.idleTimer = time.AfterFunc(a.opts.idleTimeout, func() {
		a.lifecycleMu.Lock()
		if a.state.Load() != started || a.idleGeneration != generation {
			a.lifecycleMu.Unlock()
			return
		}
		a.idleTimer = nil
		a.lifecycleMu.Unlock()

		if a.opts.releaseGuard != nil && !a.opts.releaseGuard(a) {
			return
		}

		a.lifecycleMu.Lock()
		if a.state.Load() != started || a.idleGeneration != generation {
			a.lifecycleMu.Unlock()
			return
		}
		a.idleGeneration++
		a.lifecycleMu.Unlock()

		a.Destroy()
	})
	a.lifecycleMu.Unlock()
}

func (a *Actor) cancelIdleRelease() {
	a.lifecycleMu.Lock()
	a.idleGeneration++
	if a.idleTimer != nil {
		a.idleTimer.Stop()
		a.idleTimer = nil
	}
	a.lifecycleMu.Unlock()
}

// Spawn 衍生出一个Actor
func (a *Actor) Spawn(creator Creator, opts ...ActorOption) (*Actor, error) {
	return a.scheduler.spawn(creator, opts...)
}

// Proxy 获取代理API
func (a *Actor) Proxy() *Proxy {
	return a.scheduler.node.proxy
}

// Invoke 调用函数（Actor内线程安全）
func (a *Actor) Invoke(fn func()) {
	a.TryInvoke(fn)
}

// TryInvoke 调用函数并返回是否成功投递到仍处于 started 状态的 Actor。
func (a *Actor) TryInvoke(fn func()) bool {
	a.rw.RLock()
	defer a.rw.RUnlock()

	if a.state.Load() != started {
		return false
	}

	a.fnChan <- fn
	return true
}

// AfterFunc 延迟调用，与官方的time.AfterFunc用法一致
func (a *Actor) AfterFunc(d time.Duration, f func()) *Timer {
	if a.state.Load() != started {
		return nil
	}

	timer := time.AfterFunc(d, func() {
		a.rw.RLock()
		defer a.rw.RUnlock()

		if a.state.Load() != started {
			return
		}

		f()
	})

	return &Timer{timer: timer}
}

// AfterInvoke 延迟调用（线程安全）
func (a *Actor) AfterInvoke(d time.Duration, f func()) *Timer {
	if a.state.Load() != started {
		return nil
	}

	timer := time.AfterFunc(d, func() {
		a.rw.RLock()
		defer a.rw.RUnlock()

		if a.state.Load() != started {
			return
		}

		a.fnChan <- f
	})

	return &Timer{timer: timer}
}

// SetDefaultRouteHandler 设置默认路由处理器
func (a *Actor) SetDefaultRouteHandler(handler RouteHandler) {
	a.rw.RLock()
	defer a.rw.RUnlock()

	switch a.state.Load() {
	case unstart:
		a.defaultRouteHandler = handler
	case started:
		a.fnChan <- func() {
			a.defaultRouteHandler = handler
		}
	default:
		// ignore
	}
}

// AddRouteHandler 添加路由处理器
func (a *Actor) AddRouteHandler(route int32, handler RouteHandler) {
	a.rw.RLock()
	defer a.rw.RUnlock()

	switch a.state.Load() {
	case unstart:
		a.routes[route] = handler
	case started:
		a.fnChan <- func() {
			a.routes[route] = handler

			if a.opts.dispatch {
				a.scheduler.routes.Store(route, a.Kind())
			}
		}
	default:
		// ignore
	}
}

// AddEventHandler 添加事件处理器
func (a *Actor) AddEventHandler(event cluster.Event, handler EventHandler) {
	a.rw.RLock()
	defer a.rw.RUnlock()

	switch a.state.Load() {
	case unstart:
		a.events[event] = handler
	case started:
		a.fnChan <- func() {
			a.events[event] = handler
		}
	default:
		// ignore
	}
}

// Next 投递消息到Actor中进行处理
func (a *Actor) Next(ctx Context) {
	a.rw.RLock()
	defer a.rw.RUnlock()

	if a.state.Load() != started {
		return
	}

	ctx.storeActor(a)

	ctx.incrVersion()

	ctx.Cancel()

	a.mailbox <- ctx
}

// Deliver 投递消息到当前Actor中进行处理
func (a *Actor) Deliver(uid int64, message *cluster.Message) error {
	buf, err := a.scheduler.node.proxy.PackBuffer(message.Data)
	if err != nil {
		return err
	}

	req := a.scheduler.node.reqPool.Get().(*request)
	req.nid = a.scheduler.node.opts.id
	req.uid = uid
	req.message.Seq = message.Seq
	req.message.Route = message.Route
	req.message.Data = buf

	a.Next(req)

	return nil
}

// Push 推送消息到本地Node队列上进行处理
func (a *Actor) Push(uid int64, message *cluster.Message) error {
	buf, err := a.scheduler.node.proxy.PackBuffer(message.Data)
	if err != nil {
		return err
	}

	a.scheduler.node.router.deliver("", a.scheduler.node.opts.id, a.PID(), 0, uid, session.Token{}, message.Seq, message.Route, buf)

	return nil
}

// Destroy 销毁Actor
func (a *Actor) Destroy() bool {
	if a == nil || a.scheduler == nil {
		return false
	}
	return a.scheduler.killActor(a)
}

// 销毁Actor
func (a *Actor) destroy() bool {
	if !a.state.CompareAndSwap(started, destroyed) {
		return false
	}

	a.cancelIdleRelease()
	a.processor.Destroy()

	a.scheduler.batchUnbindActor(func(relations map[int64]map[string]*Actor) {
		a.binds.Range(func(uid, _ any) bool {
			delete(relations[uid.(int64)], a.Kind())
			return true
		})
	})

	a.rw.Lock()

	close(a.mailbox)

	close(a.fnChan)

	clear(a.routes)

	clear(a.events)

	a.processor = nil

	a.defaultRouteHandler = nil

	a.rw.Unlock()

	if a.opts.releaseHook != nil {
		a.opts.releaseHook(a)
	}

	return true
}

// 绑定用户
func (a *Actor) bindUser(uid int64) {
	a.binds.Store(uid, struct{}{})
}

// 解绑用户
func (a *Actor) unbindUser(uid int64) bool {
	_, ok := a.binds.LoadAndDelete(uid)
	return ok
}

// 分发
func (a *Actor) dispatch() {
	for {
		select {
		case ctx, ok := <-a.mailbox:
			if !ok {
				return
			}

			version := ctx.loadVersion()

			if ctx.Kind() == Event {
				if handler, ok := a.events[ctx.Event()]; ok {
					xcall.Call(func() { handler(ctx) })

					ctx.compareVersionExecDefer(version)
				}
			} else {
				if handler, ok := a.routes[ctx.Route()]; ok {
					xcall.Call(func() { handler(ctx) })

					ctx.compareVersionExecDefer(version)
				} else if a.defaultRouteHandler != nil {
					xcall.Call(func() { a.defaultRouteHandler(ctx) })

					ctx.compareVersionExecDefer(version)
				}
			}

			ctx.compareVersionRecycle(version)
		case handle, ok := <-a.fnChan:
			if !ok {
				return
			}

			xcall.Call(handle)
		}
	}
}
