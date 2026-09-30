package ws

import (
	"errors"
	"sync"
)

var ErrWriteQueueBytesExceeded = errors.New("websocket write queue byte budget exceeded")

const protocol = "ws"

const (
	closeSig        int8 = iota // 关闭信号
	dataPacket                  // 数据包
	heartbeatPacket             // 心跳包
)

type writeQueueBudget struct {
	mu               sync.Mutex
	maxBytes         int64
	highReserveBytes int64
	queuedBytes      int64
	lowPriorityBytes int64
}

func newWriteQueueBudget(maxBytes, highReserveBytes int64) *writeQueueBudget {
	if maxBytes < 0 {
		maxBytes = 0
	}
	if highReserveBytes < 0 {
		highReserveBytes = 0
	}
	if maxBytes > 0 && highReserveBytes > maxBytes {
		highReserveBytes = maxBytes
	}
	return &writeQueueBudget{
		maxBytes:         maxBytes,
		highReserveBytes: highReserveBytes,
	}
}

func (b *writeQueueBudget) reserve(bytes int64, lowPriority bool) bool {
	if b == nil || b.maxBytes <= 0 || bytes <= 0 {
		return true
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if bytes > b.maxBytes-b.queuedBytes {
		return false
	}
	if lowPriority {
		lowLimit := b.maxBytes - b.highReserveBytes
		if bytes > lowLimit-b.lowPriorityBytes {
			return false
		}
	}

	b.queuedBytes += bytes
	if lowPriority {
		b.lowPriorityBytes += bytes
	}
	return true
}

func (b *writeQueueBudget) release(bytes int64, lowPriority bool) {
	if b == nil || b.maxBytes <= 0 || bytes <= 0 {
		return
	}

	b.mu.Lock()
	if bytes >= b.queuedBytes {
		b.queuedBytes = 0
	} else {
		b.queuedBytes -= bytes
	}
	if lowPriority {
		if bytes >= b.lowPriorityBytes {
			b.lowPriorityBytes = 0
		} else {
			b.lowPriorityBytes -= bytes
		}
	}
	b.mu.Unlock()
}

type task struct {
	typ         int8
	msg         []byte
	queuedBytes int64
	lowPriority bool
}
