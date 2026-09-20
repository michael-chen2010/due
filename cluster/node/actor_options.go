package node

import "time"

type ActorReleaseGuard func(actor *Actor) bool

type ActorReleaseHook func(actor *Actor)

type actorOptions struct {
	id           string            // Actor编号
	kind         string            // Actor类型
	args         []any             // 传递到Processor中的参数
	wait         bool              // 是否需要等待
	dispatch     bool              // 是否接受调度器调度
	idleTimeout  time.Duration     // 空闲释放超时；<=0 表示禁用自动释放
	releaseGuard ActorReleaseGuard // 自动释放前的最终校验
	releaseHook  ActorReleaseHook  // Actor 实际销毁后的回调
}

type ActorOption func(o *actorOptions)

func defaultActorOptions() *actorOptions {
	return &actorOptions{wait: true, dispatch: true}
}

// WithActorID 设置Actor编号
func WithActorID(id string) ActorOption {
	return func(o *actorOptions) { o.id = id }
}

// WithActorKind 设置Actor类型
func WithActorKind(kind string) ActorOption {
	return func(o *actorOptions) { o.kind = kind }
}

// WithActorArgs 设置传递到Processor中的参数
func WithActorArgs(args ...any) ActorOption {
	return func(o *actorOptions) { o.args = append(o.args, args...) }
}

// WithActorNonWait 设置Actor无需等待属性（Node组件关关闭时无需等待此Actor结束）
func WithActorNonWait() ActorOption {
	return func(o *actorOptions) { o.wait = false }
}

// WithActorNonDispatch 设置Actor不可调度
func WithActorNonDispatch() ActorOption {
	return func(o *actorOptions) { o.dispatch = false }
}

// WithActorIdleTimeout 设置 Actor 进入 Idle 后的自动释放超时。
func WithActorIdleTimeout(timeout time.Duration) ActorOption {
	return func(o *actorOptions) {
		if timeout > 0 {
			o.idleTimeout = timeout
		}
	}
}

// WithActorReleaseGuard 设置 Idle 超时释放前的最终校验。
func WithActorReleaseGuard(guard ActorReleaseGuard) ActorOption {
	return func(o *actorOptions) {
		if guard != nil {
			o.releaseGuard = guard
		}
	}
}

// WithActorReleaseHook 设置 Actor 实际销毁后的回调。
func WithActorReleaseHook(hook ActorReleaseHook) ActorOption {
	return func(o *actorOptions) {
		if hook != nil {
			o.releaseHook = hook
		}
	}
}
