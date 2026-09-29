package ws

import (
	"testing"

	dueerrors "github.com/dobyte/due/v2/errors"
)

func TestServerPacketHeartbeatInspectionDefaultsEnabled(t *testing.T) {
	opts := defaultServerOptions()
	if !opts.packetHeartbeatInspection {
		t.Fatal("packet heartbeat inspection must stay enabled by default")
	}

	isHeartbeat, err := checkServerHeartbeat(opts, []byte{0x08, 0x01})
	if !dueerrors.Is(err, dueerrors.ErrInvalidMessage) {
		t.Fatalf("default inspection error = %v, want %v", err, dueerrors.ErrInvalidMessage)
	}
	if isHeartbeat {
		t.Fatal("invalid raw payload reported as heartbeat")
	}
}

func TestServerPacketHeartbeatInspectionCanBeDisabled(t *testing.T) {
	opts := defaultServerOptions()
	WithServerPacketHeartbeatInspection(false)(opts)

	if opts.packetHeartbeatInspection {
		t.Fatal("packet heartbeat inspection remains enabled")
	}

	raw := []byte{0x08, 0x01}
	isHeartbeat, err := checkServerHeartbeat(opts, raw)
	if err != nil {
		t.Fatalf("disabled inspection rejected raw payload: %v", err)
	}
	if isHeartbeat {
		t.Fatal("raw payload reported as heartbeat when inspection is disabled")
	}
}
