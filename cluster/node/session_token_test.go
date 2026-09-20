package node

import (
	"testing"

	"github.com/dobyte/due/v2/session"
)

func TestRequestSessionToken(t *testing.T) {
	want := session.Token{UID: 1001, Generation: 7}
	req := &request{token: want}
	if got := req.SessionToken(); got != want {
		t.Fatalf("request token=%+v, want %+v", got, want)
	}
}

func TestEventSessionToken(t *testing.T) {
	want := session.Token{UID: 1001, Generation: 7}
	evt := &event{token: want}
	if got := evt.SessionToken(); got != want {
		t.Fatalf("event token=%+v, want %+v", got, want)
	}
}
