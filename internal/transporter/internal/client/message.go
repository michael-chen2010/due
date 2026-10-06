package client

import (
	"sync/atomic"

	"github.com/dobyte/due/v2/core/buffer"
)

const (
	statePending   = 0 // 待发送
	stateSent      = 1 // 已发送，等待响应
	stateCanceled  = 2 // 已取消
	stateDelivered = 3 // 响应已交付
	stateFailed    = 4 // 连接写入失败
)

type callState struct {
	seq   uint64
	call  chan buffer.Buffer
	state atomic.Int32
}

func newCallState(seq uint64) *callState {
	state := &callState{
		seq:  seq,
		call: make(chan buffer.Buffer, 1),
	}
	state.state.Store(statePending)
	return state
}

func (s *callState) deliver(buf buffer.Buffer) {
	if s.state.CompareAndSwap(stateSent, stateDelivered) {
		s.call <- buf
		return
	}
	buf.Release()
}

func (s *callState) fail() {
	if s.state.CompareAndSwap(stateSent, stateFailed) {
		close(s.call)
	}
}

func (s *callState) cancel(pending *pending) {
	for {
		switch s.state.Load() {
		case statePending:
			if s.state.CompareAndSwap(statePending, stateCanceled) {
				return
			}
		case stateSent:
			if s.state.CompareAndSwap(stateSent, stateCanceled) {
				pending.delete(s.seq)
				return
			}
		case stateDelivered:
			if buf, ok := <-s.call; ok {
				buf.Release()
			}
			return
		case stateFailed, stateCanceled:
			return
		}
	}
}

type message struct {
	buf  *buffer.NocopyBuffer // 数据buffer
	call *callState           // 同步调用状态；异步 Send 为 nil
}
