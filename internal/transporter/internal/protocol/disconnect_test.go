package protocol_test

import (
	"github.com/dobyte/due/v2/internal/transporter/internal/codes"
	"github.com/dobyte/due/v2/internal/transporter/internal/protocol"
	"github.com/dobyte/due/v2/session"
	"testing"
)

func TestEncodeDisconnectReq(t *testing.T) {
	buffer := protocol.EncodeDisconnectReq(1, session.User, 3, true)

	t.Log(buffer.Bytes())
}

func TestDecodeDisconnectReq(t *testing.T) {
	buffer := protocol.EncodeDisconnectReq(1, session.User, 3, false)

	seq, kind, target, force, err := protocol.DecodeDisconnectReq(buffer.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("seq: %v", seq)
	t.Logf("kind: %v", kind)
	t.Logf("target: %v", target)
	t.Logf("force: %v", force)
}

func TestEncodeDecodeDisconnectCurrentReqCarriesSessionToken(t *testing.T) {
	wantToken := session.Token{UID: 3003, Generation: 77}
	buf := protocol.EncodeDisconnectCurrentReq(
		9,
		session.User,
		wantToken.UID,
		true,
		wantToken,
	)

	seq, kind, target, force, token, err :=
		protocol.DecodeDisconnectReqWithToken(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if seq != 9 ||
		kind != session.User ||
		target != wantToken.UID ||
		!force ||
		token != wantToken {
		t.Fatalf(
			"decoded seq/kind/target/force/token=%d/%v/%d/%v/%+v",
			seq,
			kind,
			target,
			force,
			token,
		)
	}

	legacy := protocol.EncodeDisconnectReq(10, session.User, 3004, false)
	seq, kind, target, force, token, err =
		protocol.DecodeDisconnectReqWithToken(legacy.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if seq != 10 ||
		kind != session.User ||
		target != 3004 ||
		force ||
		token != (session.Token{}) {
		t.Fatalf(
			"legacy decoded seq/kind/target/force/token=%d/%v/%d/%v/%+v",
			seq,
			kind,
			target,
			force,
			token,
		)
	}
}

func TestEncodeDisconnectRes(t *testing.T) {
	buffer := protocol.EncodeDisconnectRes(1, codes.OK)

	t.Log(buffer.Bytes())
}

func TestDecodeDisconnectRes(t *testing.T) {
	buffer := protocol.EncodeDisconnectRes(1, codes.OK)

	code, err := protocol.DecodeDisconnectRes(buffer.Bytes())
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("code: %v", code)
}
