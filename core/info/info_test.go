package info

import (
	"strings"
	"testing"
)

func TestRowsKeepLegacyOutput(t *testing.T) {
	for _, text := range []string{"", "ID: demo", "Name: 游戏", strings.Repeat("x", 52)} {
		t.Run(text, func(t *testing.T) {
			want := "| " + text + strings.Repeat(" ", 53-strLen(text)) + "|"
			if got := buildRowInfo(text); got != want {
				t.Fatalf("row = %q; want %q", got, want)
			}
		})
	}
}

func TestLongRowPreservesAllText(t *testing.T) {
	for _, text := range []string{strings.Repeat("x", 54), "Name: webgame.multiplayer.v1.pp-s02-20261021.pingpong-pals.tabletennis.pc-1v1.local.lobby", strings.Repeat("游", 64)} {
		t.Run(text, func(t *testing.T) {
			if got, want := buildRowInfo(text), "| "+text+"|"; got != want {
				t.Fatalf("row = %q; want %q", got, want)
			}
		})
	}
}

func TestTitleKeepsLegacyOutput(t *testing.T) {
	if got, want := buildTopBorder("Mesh"), "┌"+strings.Repeat("─", 25)+"Mesh"+strings.Repeat("─", 25)+"┐"; got != want {
		t.Fatalf("title = %q; want %q", got, want)
	}
}

func TestLongTitlePreservesAllText(t *testing.T) {
	for _, title := range []string{strings.Repeat("x", 55), strings.Repeat("游", 64)} {
		t.Run(title, func(t *testing.T) {
			if got, want := buildTopBorder(title), "┌"+title+"┐"; got != want {
				t.Fatalf("title = %q; want %q", got, want)
			}
		})
	}
}
