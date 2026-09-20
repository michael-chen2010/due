package protocol_test

import (
	"github.com/dobyte/due/v2/cluster"
	"github.com/dobyte/due/v2/internal/transporter/internal/codes"
	"github.com/dobyte/due/v2/internal/transporter/internal/protocol"
	"testing"
)

func TestEncodeTriggerReq(t *testing.T) {
	buffer := protocol.EncodeTriggerReq(1, cluster.Disconnect, 1, 2, 7)

	t.Log(buffer.Bytes())
}

func TestDecodeTriggerReq(t *testing.T) {
	const generation uint64 = 7
	buffer := protocol.EncodeTriggerReq(1, cluster.Disconnect, 1, 2, generation)

	seq, evt, cid, uid, gotGeneration, err := protocol.DecodeTriggerReq(buffer.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if seq != 1 || evt != cluster.Disconnect || cid != 1 || uid != 2 || gotGeneration != generation {
		t.Fatalf("decoded seq=%d evt=%v cid=%d uid=%d generation=%d", seq, evt, cid, uid, gotGeneration)
	}
}

func TestEncodeTriggerRes(t *testing.T) {
	buffer := protocol.EncodeTriggerRes(1, codes.OK)

	t.Log(buffer.Bytes())
}

func TestDecodeTriggerRes(t *testing.T) {
	buffer := protocol.EncodeTriggerRes(1, codes.OK)

	code, err := protocol.DecodeTriggerRes(buffer.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("code: %v", code)
}
