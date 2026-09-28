package protocol_test

import (
	"testing"
	"time"

	"github.com/dobyte/due/v2/cluster"
	"github.com/dobyte/due/v2/core/buffer"
	"github.com/dobyte/due/v2/internal/transporter/internal/protocol"
)

func TestDeliverRequestMetadataRoundTrip(t *testing.T) {
	deadline := time.UnixMilli(1790604000123)
	meta := cluster.RequestMetadata{
		Deadline:      deadline,
		CorrelationID: "corr-m0-123",
	}
	buf := protocol.EncodeDeliverReqWithMetadata(
		1,
		2,
		3,
		7,
		meta,
		buffer.NewNocopyBuffer([]byte("hello world")),
	)

	seq, cid, uid, generation, gotMeta, message, err := protocol.DecodeDeliverReqWithMetadata(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if seq != 1 || cid != 2 || uid != 3 || generation != 7 {
		t.Fatalf("decoded seq=%d cid=%d uid=%d generation=%d", seq, cid, uid, generation)
	}
	if !gotMeta.Deadline.Equal(deadline) {
		t.Fatalf("deadline=%v, want %v", gotMeta.Deadline, deadline)
	}
	if gotMeta.CorrelationID != meta.CorrelationID {
		t.Fatalf("correlation_id=%q, want %q", gotMeta.CorrelationID, meta.CorrelationID)
	}
	if string(message) != "hello world" {
		t.Fatalf("message=%q, want hello world", string(message))
	}
}
