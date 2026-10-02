package due

import "testing"

func TestNewSignalChannelIsBuffered(t *testing.T) {
	sig := newSignalChannel()
	if got := cap(sig); got != 1 {
		t.Fatalf("signal channel capacity=%d, want 1", got)
	}
}
