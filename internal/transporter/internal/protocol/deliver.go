package protocol

import (
	"encoding/binary"
	"io"
	"time"

	"github.com/dobyte/due/v2/cluster"
	"github.com/dobyte/due/v2/core/buffer"
	"github.com/dobyte/due/v2/errors"
	"github.com/dobyte/due/v2/internal/transporter/internal/route"
)

const (
	deliverReqBytes               = defaultSizeBytes + defaultHeaderBytes + defaultRouteBytes + defaultSeqBytes + b64 + b64 + b64 + b64 + b16
	deliverResBytes               = defaultSizeBytes + defaultHeaderBytes + defaultRouteBytes + defaultSeqBytes + defaultCodeBytes
	deliverTraceContextFixedBytes = b8 + b16 + b16
	deliverTraceContextVersion    = uint8(1)
	maxTraceContextFieldBytes     = 1<<16 - 1
)

// EncodeDeliverReq 编码投递消息请求。
// 兼容旧调用方：metadata 使用零值。
func EncodeDeliverReq(seq uint64, cid int64, uid int64, generation uint64, buf buffer.Buffer) *buffer.NocopyBuffer {
	return EncodeDeliverReqWithMetadata(seq, cid, uid, generation, cluster.RequestMetadata{}, buf)
}

// EncodeDeliverReqWithMetadata 编码投递消息请求。
// Legacy 协议：size + header + route + seq + cid + uid + generation +
// deadline_unix_ms + correlation_id_len + correlation_id + <message packet>。
// Trace Context 非空时 header 置 traceContextBit，并在 correlation_id 后追加：
// version + traceparent_len + tracestate_len + traceparent + tracestate。
func EncodeDeliverReqWithMetadata(
	seq uint64,
	cid int64,
	uid int64,
	generation uint64,
	metadata cluster.RequestMetadata,
	buf buffer.Buffer,
) *buffer.NocopyBuffer {
	correlationID := metadata.CorrelationID
	traceParent := metadata.TraceParent
	traceState := metadata.TraceState
	hasTraceContext := traceParent != "" &&
		len(traceParent) <= maxTraceContextFieldBytes &&
		len(traceState) <= maxTraceContextFieldBytes
	if !hasTraceContext {
		traceParent = ""
		traceState = ""
	}

	size := deliverReqBytes + len(correlationID)
	header := dataBit
	if hasTraceContext {
		header |= traceContextBit
		size += deliverTraceContextFixedBytes + len(traceParent) + len(traceState)
	}

	var deadlineUnixMilli int64
	if !metadata.Deadline.IsZero() {
		deadlineUnixMilli = metadata.Deadline.UnixMilli()
	}

	writer := buffer.MallocWriter(size)
	writer.WriteUint32s(binary.BigEndian, uint32(size-defaultSizeBytes+buf.Len()))
	writer.WriteUint8s(header)
	writer.WriteUint8s(route.Deliver)
	writer.WriteUint64s(binary.BigEndian, seq)
	writer.WriteInt64s(binary.BigEndian, cid, uid)
	writer.WriteUint64s(binary.BigEndian, generation)
	writer.WriteInt64s(binary.BigEndian, deadlineUnixMilli)
	writer.WriteUint16s(binary.BigEndian, uint16(len(correlationID)))
	writer.WriteString(correlationID)
	if hasTraceContext {
		writer.WriteUint8s(deliverTraceContextVersion)
		writer.WriteUint16s(binary.BigEndian, uint16(len(traceParent)), uint16(len(traceState)))
		writer.WriteString(traceParent)
		writer.WriteString(traceState)
	}

	return buffer.NewNocopyBuffer(writer, buf)
}

// DecodeDeliverReq 解码投递消息请求。
// 兼容旧调用方：metadata 被丢弃。
func DecodeDeliverReq(data []byte) (seq uint64, cid int64, uid int64, generation uint64, message []byte, err error) {
	seq, cid, uid, generation, _, message, err = DecodeDeliverReqWithMetadata(data)
	return
}

// DecodeDeliverReqWithMetadata 解码投递消息请求。
func DecodeDeliverReqWithMetadata(data []byte) (
	seq uint64,
	cid int64,
	uid int64,
	generation uint64,
	metadata cluster.RequestMetadata,
	message []byte,
	err error,
) {
	if len(data) < deliverReqBytes {
		err = errors.ErrInvalidMessage
		return
	}

	reader := buffer.NewReader(data)

	if _, err = reader.Seek(defaultSizeBytes+defaultHeaderBytes+defaultRouteBytes, io.SeekStart); err != nil {
		return
	}

	if seq, err = reader.ReadUint64(binary.BigEndian); err != nil {
		return
	}

	if cid, err = reader.ReadInt64(binary.BigEndian); err != nil {
		return
	}

	if uid, err = reader.ReadInt64(binary.BigEndian); err != nil {
		return
	}

	if generation, err = reader.ReadUint64(binary.BigEndian); err != nil {
		return
	}

	var deadlineUnixMilli int64
	if deadlineUnixMilli, err = reader.ReadInt64(binary.BigEndian); err != nil {
		return
	}
	if deadlineUnixMilli != 0 {
		metadata.Deadline = time.UnixMilli(deadlineUnixMilli)
	}

	var correlationBytes uint16
	if correlationBytes, err = reader.ReadUint16(binary.BigEndian); err != nil {
		return
	}
	if int(correlationBytes) > len(data)-deliverReqBytes {
		err = errors.ErrInvalidMessage
		return
	}
	if correlationBytes > 0 {
		if metadata.CorrelationID, err = reader.ReadString(int(correlationBytes)); err != nil {
			return
		}
	}

	messageOffset := deliverReqBytes + int(correlationBytes)
	header := data[defaultSizeBytes]
	if header&traceContextBit == traceContextBit {
		if len(data)-messageOffset < deliverTraceContextFixedBytes {
			err = errors.ErrInvalidMessage
			return
		}
		var version uint8
		if version, err = reader.ReadUint8(); err != nil {
			return
		}
		if version != deliverTraceContextVersion {
			err = errors.ErrInvalidMessage
			return
		}
		var traceParentBytes, traceStateBytes uint16
		if traceParentBytes, err = reader.ReadUint16(binary.BigEndian); err != nil {
			return
		}
		if traceStateBytes, err = reader.ReadUint16(binary.BigEndian); err != nil {
			return
		}
		traceBytes := int(traceParentBytes) + int(traceStateBytes)
		if traceParentBytes == 0 || traceBytes > len(data)-messageOffset-deliverTraceContextFixedBytes {
			err = errors.ErrInvalidMessage
			return
		}
		if metadata.TraceParent, err = reader.ReadString(int(traceParentBytes)); err != nil {
			return
		}
		if traceStateBytes > 0 {
			if metadata.TraceState, err = reader.ReadString(int(traceStateBytes)); err != nil {
				return
			}
		}
		messageOffset += deliverTraceContextFixedBytes + traceBytes
	}

	message = data[messageOffset:]

	return
}

// EncodeDeliverRes 编码投递消息响应
// 协议：size + header + route + seq + code
func EncodeDeliverRes(seq uint64, code uint16) *buffer.NocopyBuffer {
	writer := buffer.MallocWriter(deliverResBytes)
	writer.WriteUint32s(binary.BigEndian, uint32(deliverResBytes-defaultSizeBytes))
	writer.WriteUint8s(dataBit)
	writer.WriteUint8s(route.Deliver)
	writer.WriteUint64s(binary.BigEndian, seq)
	writer.WriteUint16s(binary.BigEndian, code)

	return buffer.NewNocopyBuffer(writer)
}

// DecodeDeliverRes 解码投递消息响应
// 协议：size + header + route + seq + code
func DecodeDeliverRes(data []byte) (code uint16, err error) {
	if len(data) != deliverResBytes {
		err = errors.ErrInvalidMessage
		return
	}

	reader := buffer.NewReader(data)

	if _, err = reader.Seek(-defaultCodeBytes, io.SeekEnd); err != nil {
		return
	}

	if code, err = reader.ReadUint16(binary.BigEndian); err != nil {
		return
	}

	return
}
