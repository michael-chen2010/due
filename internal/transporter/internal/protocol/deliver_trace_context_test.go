package protocol_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/dobyte/due/v2/cluster"
	"github.com/dobyte/due/v2/core/buffer"
	"github.com/dobyte/due/v2/internal/transporter/internal/protocol"
)

func TestDeliverRequestTraceContextRoundTrip(t *testing.T) {
	meta := cluster.RequestMetadata{
		Deadline:      time.UnixMilli(1791262000123),
		CorrelationID: "corr-trace-123",
		TraceParent:   "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		TraceState:    "vendor=value",
	}
	buf := protocol.EncodeDeliverReqWithMetadata(
		11,
		22,
		33,
		9,
		meta,
		buffer.NewNocopyBuffer([]byte("trace-message")),
	)

	seq, cid, uid, generation, got, message, err := protocol.DecodeDeliverReqWithMetadata(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if seq != 11 || cid != 22 || uid != 33 || generation != 9 {
		t.Fatalf("identity seq=%d cid=%d uid=%d generation=%d", seq, cid, uid, generation)
	}
	if got.CorrelationID != meta.CorrelationID || got.TraceParent != meta.TraceParent || got.TraceState != meta.TraceState {
		t.Fatalf("metadata=%+v, want %+v", got, meta)
	}
	if got.Deadline.UnixMilli() != meta.Deadline.UnixMilli() {
		t.Fatalf("deadline=%v, want %v", got.Deadline, meta.Deadline)
	}
	if string(message) != "trace-message" {
		t.Fatalf("message=%q", string(message))
	}
	if buf.Bytes()[4] == 0 {
		t.Fatal("extended trace-context request did not set a header extension bit")
	}
}

func TestDeliverRequestWithoutTraceContextKeepsLegacyHeaderAndMessageLayout(t *testing.T) {
	meta := cluster.RequestMetadata{
		Deadline:      time.UnixMilli(1791262000456),
		CorrelationID: "corr-legacy-123",
	}
	buf := protocol.EncodeDeliverReqWithMetadata(
		1,
		2,
		3,
		4,
		meta,
		buffer.NewNocopyBuffer([]byte("legacy-message")),
	)
	if buf.Bytes()[4] != 0 {
		t.Fatalf("legacy header=%08b, want zero data header", buf.Bytes()[4])
	}

	_, _, _, _, got, message, err := protocol.DecodeDeliverReqWithMetadata(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if got.TraceParent != "" || got.TraceState != "" {
		t.Fatalf("legacy decode unexpectedly produced trace context: %+v", got)
	}
	if string(message) != "legacy-message" {
		t.Fatalf("legacy message=%q", string(message))
	}
}

func TestDeliverRequestRejectsUnknownTraceContextVersion(t *testing.T) {
	meta := cluster.RequestMetadata{
		CorrelationID: "corr-trace-version",
		TraceParent:   "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		TraceState:    "vendor=value",
	}
	buf := protocol.EncodeDeliverReqWithMetadata(
		1,
		2,
		3,
		4,
		meta,
		buffer.NewNocopyBuffer([]byte("message")),
	)
	data := append([]byte(nil), buf.Bytes()...)
	traceParentOffset := bytes.Index(data, []byte(meta.TraceParent))
	if traceParentOffset < 5 {
		t.Fatalf("traceparent offset=%d", traceParentOffset)
	}
	data[traceParentOffset-5] = 2

	if _, _, _, _, _, _, err := protocol.DecodeDeliverReqWithMetadata(data); err == nil {
		t.Fatal("DecodeDeliverReqWithMetadata() accepted unknown trace context version")
	}
}
