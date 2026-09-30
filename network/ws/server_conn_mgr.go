/**
 * @Author: fuxiao
 * @Email: 576101059@qq.com
 * @Date: 2022/5/28 3:48 下午
 * @Desc: 连接管理器
 */

package ws

import (
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/network"
	"github.com/dobyte/due/v2/utils/xcall"
	"github.com/gorilla/websocket"
)

type serverConnMgr struct {
	id          atomic.Int64      // 连接ID
	total       atomic.Int64      // 总连接数
	server      *server           // 服务器
	writeBudget *writeQueueBudget // Gate 级共享写队列字节预算
	pool        sync.Pool         // 连接池
	partitions  []*partition      // 连接管理
}

func newConnMgr(server *server) *serverConnMgr {
	cm := &serverConnMgr{}
	cm.server = server
	cm.writeBudget = newWriteQueueBudget(
		server.opts.maxWriteQueueBytes,
		server.opts.highPriorityReserveBytes,
	)
	cm.pool = sync.Pool{New: func() any { return &serverConn{taskPool: sync.Pool{New: func() any { return &task{} }}} }}
	cm.partitions = make([]*partition, 10)

	for i := 0; i < len(cm.partitions); i++ {
		cm.partitions[i] = &partition{connections: make(map[*websocket.Conn]*serverConn)}
	}

	return cm
}

// 关闭连接
func (cm *serverConnMgr) close() {
	var wg sync.WaitGroup

	wg.Add(len(cm.partitions))

	for i := range cm.partitions {
		p := cm.partitions[i]

		xcall.Go(func() {
			p.close()
			wg.Done()
		})
	}

	wg.Wait()
}

// 分配连接
func (cm *serverConnMgr) allocate(c *websocket.Conn) error {
	if !cm.reserve() {
		return errors.ErrTooManyConnection
	}

	id := cm.id.Add(1)
	conn := cm.pool.Get().(*serverConn)
	conn.init(cm, id, c)

	// init 会启动读写协程并同步触发 OnConnect；上层可能在 OnConnect
	// 内立即关闭连接。此时连接尚未登记，recycle 找不到它，因此由
	// allocate 负责归还预占槽位，并且不能再把已关闭连接登记进去。
	//
	// 对仍然打开的连接，在 conn.rw 下完成状态复核与登记。若读协程
	// 已经把状态切到 Closed，它会等待同一把锁；登记完成后 recycle
	// 就能正常找到连接并归还槽位。
	conn.rw.Lock()
	if conn.State() != network.ConnOpened {
		conn.rw.Unlock()
		cm.total.Add(-1)
		return nil
	}
	index := int(reflect.ValueOf(c).Pointer()) % len(cm.partitions)
	cm.partitions[index].store(c, conn)
	conn.rw.Unlock()

	return nil
}

func (cm *serverConnMgr) reserve() bool {
	maxConnNum := int64(cm.server.opts.maxConnNum)
	for {
		total := cm.total.Load()
		if total >= maxConnNum {
			return false
		}
		if cm.total.CompareAndSwap(total, total+1) {
			return true
		}
	}
}

// 回收连接
func (cm *serverConnMgr) recycle(c *websocket.Conn) {
	index := int(reflect.ValueOf(c).Pointer()) % len(cm.partitions)
	if conn, ok := cm.partitions[index].delete(c); ok {
		conn.reset()
		cm.pool.Put(conn)
		cm.total.Add(-1)
	}
}

type partition struct {
	rw          sync.RWMutex
	connections map[*websocket.Conn]*serverConn
}

// 存储连接
func (p *partition) store(c *websocket.Conn, conn *serverConn) {
	p.rw.Lock()
	p.connections[c] = conn
	p.rw.Unlock()
}

// 删除连接
func (p *partition) delete(c *websocket.Conn) (*serverConn, bool) {
	p.rw.Lock()
	conn, ok := p.connections[c]
	if ok {
		delete(p.connections, c)
	}
	p.rw.Unlock()

	return conn, ok
}

// 关闭该分片内的所有连接
func (p *partition) close() {
	for _, conn := range p.connections {
		_ = conn.Close()
	}
}
