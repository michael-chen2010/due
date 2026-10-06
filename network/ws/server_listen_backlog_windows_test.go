//go:build windows

package ws

import "testing"

func TestWindowsListenBacklogUsesSomaxconnHintEncoding(t *testing.T) {
	for _, tc := range []struct {
		backlog int
		want    int
	}{
		{backlog: 200, want: -200},
		{backlog: 4096, want: -4096},
		{backlog: 65535, want: -65535},
	} {
		if got := windowsListenBacklog(tc.backlog); got != tc.want {
			t.Fatalf("windowsListenBacklog(%d) = %d, want %d", tc.backlog, got, tc.want)
		}
	}
}
