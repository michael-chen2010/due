package config_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dobyte/due/v2/config"
	"github.com/dobyte/due/v2/config/file"
)

func configureTest(t *testing.T, files map[string][]byte) {
	t.Helper()
	dir := t.TempDir()
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), content, 0o600); err != nil {
			t.Fatalf("write config fixture %s: %v", name, err)
		}
	}

	source := file.NewSource(
		file.WithPath(dir),
		file.WithMode(config.ReadWrite),
	)
	config.SetConfigurator(config.NewConfigurator(config.WithSources(source)))
	t.Cleanup(func() {
		config.SetConfigurator(nil)
	})
}

func TestWatch(t *testing.T) {
	configureTest(t, map[string][]byte{
		"config.json": []byte(`{"timezone":"Local","pid":"./run/gate.pid"}`),
	})
	ticker1 := time.NewTicker(2 * time.Second)
	ticker2 := time.After(5 * time.Second)

	for {
		select {
		case <-ticker1.C:
			t.Log(config.Get("config.timezone").String())
			t.Log(config.Get("config.pid").String())
		case <-ticker2:
			config.Close()
			return
		}
	}
}

func TestStore(t *testing.T) {
	configureTest(t, nil)
	ctx := context.Background()
	filename := "config.json"
	content1 := map[string]any{
		"timezone": "Local",
	}

	content2 := map[string]any{
		"timezone": "UTC",
		"pid":      "./run/gate.pid",
	}

	err := config.Store(ctx, file.Name, filename, content1, true)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(5 * time.Second)

	err = config.Store(ctx, file.Name, filename, content2)
	if err != nil {
		t.Fatal(err)
	}
}

func TestLoad(t *testing.T) {
	configureTest(t, map[string][]byte{
		"config.json": []byte(`{"timezone":"UTC","pid":"./run/gate.pid"}`),
	})
	ctx := context.Background()
	filename := "config.json"
	c, err := config.Load(ctx, file.Name, filename)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(c[0].Name)
	t.Log(c[0].Path)
	t.Log(c[0].Format)
	t.Log(c[0].Content)
}

func BenchmarkGet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		config.Get("config").Value()
	}
}
