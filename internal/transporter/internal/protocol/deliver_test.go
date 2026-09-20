package protocol_test

import (
	"testing"

	"github.com/dobyte/due/v2/core/buffer"
	"github.com/dobyte/due/v2/internal/transporter/internal/codes"
	"github.com/dobyte/due/v2/internal/transporter/internal/protocol"
)

func TestEncodeDeliverReq(t *testing.T) {
	buffer := protocol.EncodeDeliverReq(1, 2, 3, 7, buffer.NewNocopyBuffer([]byte("hello world")))

	t.Log(buffer.Bytes())
}

func TestDecodeDeliverReq(t *testing.T) {
	const generation uint64 = 7
	buffer := protocol.EncodeDeliverReq(1, 2, 3, generation, buffer.NewNocopyBuffer([]byte("hello world")))

	seq, cid, uid, gotGeneration, message, err := protocol.DecodeDeliverReq(buffer.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if seq != 1 || cid != 2 || uid != 3 || gotGeneration != generation || string(message) != "hello world" {
		t.Fatalf("decoded seq=%d cid=%d uid=%d generation=%d message=%q", seq, cid, uid, gotGeneration, string(message))
	}
}

func TestEncodeDeliverRes(t *testing.T) {
	buffer := protocol.EncodeDeliverRes(1, codes.OK)

	t.Log(buffer.Bytes())
}

func TestDecodeDeliverRes(t *testing.T) {
	buffer := protocol.EncodePushRes(1, codes.OK)

	code, err := protocol.DecodeDeliverRes(buffer.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("code: %v", code)
}
