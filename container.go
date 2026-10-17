package due

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dobyte/due/v2/cache"
	"github.com/dobyte/due/v2/component"
	"github.com/dobyte/due/v2/config"
	"github.com/dobyte/due/v2/core/info"
	"github.com/dobyte/due/v2/etc"
	"github.com/dobyte/due/v2/eventbus"
	"github.com/dobyte/due/v2/lock"
	"github.com/dobyte/due/v2/log"
	"github.com/dobyte/due/v2/task"
	"github.com/dobyte/due/v2/utils/xcall"
	"github.com/dobyte/due/v2/utils/xos"
)

const (
	defaultPIDKey                 = "etc.pid"                 // 进程文件路径
	defaultShutdownMaxWaitTimeKey = "etc.shutdownMaxWaitTime" // 容器关闭最大等待时间
	defaultDestroyMaxWaitTime     = 5 * time.Second
)

type Container struct {
	components []component.Component

	shutdownInit sync.Once
	shutdownOnce sync.Once
	shutdownCh   chan struct{}

	shutdownFinishOnce sync.Once
	shutdownMu         sync.Mutex
	shutdownErr        error
}

// NewContainer 创建一个容器
func NewContainer() *Container {
	c := &Container{}
	c.shutdownSignal()
	return c
}

// Add 添加组件
func (c *Container) Add(components ...component.Component) {
	c.components = append(c.components, components...)
}

// Shutdown 请求容器按正常 Close -> Destroy 生命周期退出。
func (c *Container) Shutdown() {
	if c == nil {
		return
	}

	ch := c.shutdownSignal()
	c.shutdownOnce.Do(func() {
		close(ch)
	})
}

func (c *Container) shutdownSignal() chan struct{} {
	c.shutdownInit.Do(func() {
		c.shutdownCh = make(chan struct{})
	})
	return c.shutdownCh
}

// Serve 启动容器
func (c *Container) Serve(once ...bool) {
	c.doSaveProcessID()

	c.doPrintFrameworkInfo()

	c.doInitComponents()

	c.doStartComponents()

	if len(once) == 0 || !once[0] {
		c.doWaitSystemSignal()
	}

	if err := c.finishShutdown(); err != nil {
		log.Errorf("%v", err)
		// Close/Destroy may still be running in a non-cooperative component.
		// Never start Destroy after a failed Close, or clear its live modules.
		// The caller must terminate the failed process, not advertise success.
		return
	}
	c.doClearModules()
}

// ShutdownError is sticky once a Close or Destroy barrier fails.
// Callers must treat a non-nil result as an unsuccessful process shutdown.
func (c *Container) ShutdownError() error {
	if c == nil {
		return nil
	}
	c.shutdownMu.Lock()
	defer c.shutdownMu.Unlock()
	return c.shutdownErr
}

// finishShutdown is one-shot: even if a timed-out Close eventually finishes,
// a later invocation cannot run Destroy against its uncertain lifecycle.
func (c *Container) finishShutdown() error {
	c.shutdownFinishOnce.Do(func() {
		budget := etc.Get(defaultShutdownMaxWaitTimeKey).Duration()
		if budget <= 0 {
			budget = defaultDestroyMaxWaitTime
		}
		deadline := time.Now().Add(budget)
		if err := c.doCloseComponentsWithin(budget); err != nil {
			c.shutdownMu.Lock()
			c.shutdownErr = fmt.Errorf("container close components exceeded shutdown deadline: %w", err)
			c.shutdownMu.Unlock()
			return
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			c.shutdownMu.Lock()
			c.shutdownErr = fmt.Errorf("container shutdown deadline before destroy: %w", context.DeadlineExceeded)
			c.shutdownMu.Unlock()
			return
		}
		if err := c.doDestroyComponentsWithin(remaining); err != nil {
			c.shutdownMu.Lock()
			c.shutdownErr = fmt.Errorf("container destroy components exceeded shutdown deadline: %w", err)
			c.shutdownMu.Unlock()
		}
	})
	return c.ShutdownError()
}

// 初始化所有组件
func (c *Container) doInitComponents() {
	for _, comp := range c.components {
		comp.Init()
	}
}

// 启动所有组件
func (c *Container) doStartComponents() {
	for _, comp := range c.components {
		comp.Start()
	}
}

// 关闭所有组件
func (c *Container) doCloseComponents() error {
	return c.doCloseComponentsWithin(etc.Get(defaultShutdownMaxWaitTimeKey).Duration())
}

func (c *Container) doCloseComponentsWithin(timeout time.Duration) error {
	g := xcall.NewGoroutines()
	closed := make([]atomic.Bool, len(c.components))

	for i, comp := range c.components {
		index, current := i, comp
		g.Add(func() {
			current.Close()
			closed[index].Store(true)
		})
	}

	err := g.Run(context.Background(), timeout)
	if err == nil {
		return nil
	}

	// Keep the existing deadline and Close/Destroy semantics unchanged. When
	// Close times out, name the component still holding the shutdown barrier,
	// so an operational log can distinguish Node from Gate/Mesh/other Close.
	var pending []string
	for i, comp := range c.components {
		if !closed[i].Load() {
			label := fmt.Sprintf("%s(%T)", comp.Name(), comp)
			if diagnostic, ok := comp.(interface{ ShutdownCloseStatus() string }); ok {
				label += " " + diagnostic.ShutdownCloseStatus()
			}
			pending = append(pending, label)
		}
	}
	if len(pending) == 0 {
		return err
	}
	sort.Strings(pending)
	return fmt.Errorf("%w: pending close components=%v", err, pending)
}

// 销毁所有组件
func (c *Container) doDestroyComponents() error {
	timeout := etc.Get(defaultShutdownMaxWaitTimeKey).Duration()
	if timeout <= 0 {
		timeout = defaultDestroyMaxWaitTime
	}
	return c.doDestroyComponentsWithin(timeout)
}

func (c *Container) doDestroyComponentsWithin(timeout time.Duration) error {
	g := xcall.NewGoroutines()
	for _, comp := range c.components {
		g.Add(comp.Destroy)
	}
	return g.Run(context.Background(), timeout)
}

func newSignalChannel() chan os.Signal {
	return make(chan os.Signal, 1)
}

// 等待系统信号
func (c *Container) doWaitSystemSignal() {
	sig := newSignalChannel()

	switch runtime.GOOS {
	case `windows`:
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	default:
		signal.Notify(sig, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGABRT, syscall.SIGTERM)
	}

	select {
	case s := <-sig:
		log.Warnf("process got signal %v, container will close", s)
	case <-c.shutdownSignal():
		log.Warn("container shutdown requested, container will close")
	}

	signal.Stop(sig)
}

// 清理所有模块
func (c *Container) doClearModules() {
	if err := eventbus.Close(); err != nil {
		log.Warnf("eventbus close failed: %v", err)
	}

	if err := lock.Close(); err != nil {
		log.Warnf("lock-maker close failed: %v", err)
	}

	if err := cache.Close(); err != nil {
		log.Warnf("cache close failed: %v", err)
	}

	task.Release()

	config.Close()

	etc.Close()

	log.Close()
}

// 保存进程号
func (c *Container) doSaveProcessID() {
	filename := etc.Get(defaultPIDKey).String()
	if filename == "" {
		return
	}

	if err := xos.WriteFile(filename, []byte(strconv.Itoa(syscall.Getpid()))); err != nil {
		log.Fatalf("pid save failed: %v", err)
	}
}

// 打印框架信息
func (c *Container) doPrintFrameworkInfo() {
	info.PrintFrameworkInfo()

	info.PrintGlobalInfo()
}
