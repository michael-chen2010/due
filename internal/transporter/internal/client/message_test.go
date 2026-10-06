package client_test

import (
	"testing"
	"unsafe"

	"github.com/dobyte/due/v2/core/buffer"
)

type message struct {
	buf  *buffer.NocopyBuffer // 数据buffer
	call unsafe.Pointer       // 同步调用状态指针
}

func TestMessage(t *testing.T) {
	t.Log(unsafe.Sizeof(message{}))
}
