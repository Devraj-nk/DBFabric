package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadExampleConfig(t *testing.T) {
	cfg, err := Load(filepath.Join("..", "..", "config", "shards.example.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ListenAddr != ":5433" {
		t.Errorf("ListenAddr = %q, want %q", cfg.ListenAddr, ":5433")
	}
	if cfg.HealthCheckInterval != 2*time.Second {
		t.Errorf("HealthCheckInterval = %v, want 2s", cfg.HealthCheckInterval)
	}
	if len(cfg.Shards) != 2 {
		t.Fatalf("len(Shards) = %d, want 2", len(cfg.Shards))
	}
	if cfg.Shards[0].ID != "shard-0" || cfg.Shards[0].Primary != "127.0.0.1:6543" {
		t.Errorf("Shards[0] = %+v", cfg.Shards[0])
	}
	if len(cfg.Shards[0].Replicas) != 2 {
		t.Errorf("Shards[0].Replicas = %v, want 2 entries", cfg.Shards[0].Replicas)
	}
}

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "nope.yaml")); err == nil {
		t.Fatal("expected an error for a missing file")
	}
}

func TestLoadInvalidDuration(t *testing.T) {
	path := writeConfig(t, `
listen_addr: ":5433"
health_check_interval: "not-a-duration"
shards:
  - id: shard-0
    primary: "127.0.0.1:6543"
`)
	if _, err := Load(path); err == nil {
		t.Fatal("expected an error for an invalid health_check_interval")
	}
}

func TestLoadNoShards(t *testing.T) {
	path := writeConfig(t, `
listen_addr: ":5433"
health_check_interval: "2s"
shards: []
`)
	if _, err := Load(path); err == nil {
		t.Fatal("expected an error when no shards are defined")
	}
}

func TestLoadDuplicateShardID(t *testing.T) {
	path := writeConfig(t, `
listen_addr: ":5433"
health_check_interval: "2s"
shards:
  - id: shard-0
    primary: "127.0.0.1:6543"
  - id: shard-0
    primary: "127.0.0.1:6553"
`)
	if _, err := Load(path); err == nil {
		t.Fatal("expected an error for a duplicate shard id")
	}
}

func TestLoadMissingPrimary(t *testing.T) {
	path := writeConfig(t, `
listen_addr: ":5433"
health_check_interval: "2s"
shards:
  - id: shard-0
`)
	if _, err := Load(path); err == nil {
		t.Fatal("expected an error for a shard with no primary")
	}
}
