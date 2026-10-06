package gate

import (
	"testing"

	"github.com/dobyte/due/v2/session"
)

func TestRequestDeliveryTokenClearsHistoricalTokenForUnboundConnection(t *testing.T) {
	manager := session.NewSession()
	conn := &responseFenceConn{id: 901}
	manager.AddConn(conn)

	oldToken := session.Token{UID: 7001, Generation: 11}
	if err := manager.BindToken(conn.id, oldToken.UID, oldToken); err != nil {
		t.Fatalf("bind old token: %v", err)
	}
	if _, err := manager.Unbind(oldToken.UID); err != nil {
		t.Fatalf("unbind old token: %v", err)
	}

	if retained, err := manager.Token(session.Conn, conn.id); err != nil || retained != oldToken {
		t.Fatalf("historical connection token=%+v err=%v, want retained %+v", retained, err, oldToken)
	}
	if got := requestDeliveryToken(manager, conn.id, 0); got != (session.Token{}) {
		t.Fatalf("unbound request token=%+v, want zero token", got)
	}

	newToken := session.Token{UID: oldToken.UID, Generation: 12}
	if err := manager.BindToken(conn.id, newToken.UID, newToken); err != nil {
		t.Fatalf("rebind current token: %v", err)
	}
	if got := requestDeliveryToken(manager, conn.id, newToken.UID); got != newToken {
		t.Fatalf("bound request token=%+v, want current %+v", got, newToken)
	}
}
