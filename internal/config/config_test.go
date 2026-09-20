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
	if cfg.Backend.User != "postgres" || cfg.Backend.Database != "postgres" {
		t.Errorf("Backend = %+v", cfg.Backend)
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

func TestLoadExampleConfigHealthAndRoutingDefaults(t *testing.T) {
	cfg, err := Load(filepath.Join("..", "..", "config", "shards.example.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	want := HealthConfig{
		SuspectAfterMisses: DefaultSuspectAfterMisses,
		DownAfterMisses:    DefaultDownAfterMisses,
		PgPromote:          true,
		QuorumGuard:        true,
	}
	if cfg.Health != want {
		t.Errorf("Health = %+v, want %+v", cfg.Health, want)
	}
	if cfg.Routing.DefaultMaxLagMS != DefaultMaxLagMS {
		t.Errorf("Routing.DefaultMaxLagMS = %d, want %d", cfg.Routing.DefaultMaxLagMS, DefaultMaxLagMS)
	}
}

func TestLoadOmittedHealthAndRoutingUseDefaults(t *testing.T) {
	path := writeConfig(t, `
listen_addr: ":5433"
health_check_interval: "2s"
`+validBackend+`
shards:
  - id: shard-0
    primary: "127.0.0.1:6543"
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Health.SuspectAfterMisses != DefaultSuspectAfterMisses || cfg.Health.DownAfterMisses != DefaultDownAfterMisses {
		t.Errorf("miss thresholds = %d/%d, want defaults %d/%d",
			cfg.Health.SuspectAfterMisses, cfg.Health.DownAfterMisses, DefaultSuspectAfterMisses, DefaultDownAfterMisses)
	}
	if !cfg.Health.PgPromote || !cfg.Health.QuorumGuard {
		t.Errorf("pg_promote/quorum_guard should default to true, got %+v", cfg.Health)
	}
	if cfg.Routing.DefaultMaxLagMS != DefaultMaxLagMS {
		t.Errorf("DefaultMaxLagMS = %d, want %d", cfg.Routing.DefaultMaxLagMS, DefaultMaxLagMS)
	}
}

func TestLoadExplicitFalseAndZeroOverrideDefaults(t *testing.T) {
	path := writeConfig(t, `
listen_addr: ":5433"
health_check_interval: "2s"
health:
  suspect_after_misses: 2
  down_after_misses: 5
  pg_promote: false
  quorum_guard: false
routing:
  default_max_lag_ms: 0
`+validBackend+`
shards:
  - id: shard-0
    primary: "127.0.0.1:6543"
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	want := HealthConfig{SuspectAfterMisses: 2, DownAfterMisses: 5, PgPromote: false, QuorumGuard: false}
	if cfg.Health != want {
		t.Errorf("Health = %+v, want %+v (explicit false must not be replaced by the true default)", cfg.Health, want)
	}
	if cfg.Routing.DefaultMaxLagMS != 0 {
		t.Errorf("DefaultMaxLagMS = %d, want an explicit 0 to be honored", cfg.Routing.DefaultMaxLagMS)
	}
}

func TestLoadRejectsIncoherentHealthThresholds(t *testing.T) {
	tests := map[string]string{
		"suspect below 1":    "  suspect_after_misses: 0\n  down_after_misses: 3",
		"down below suspect": "  suspect_after_misses: 3\n  down_after_misses: 2",
	}
	for name, healthBlock := range tests {
		t.Run(name, func(t *testing.T) {
			path := writeConfig(t, `
listen_addr: ":5433"
health_check_interval: "2s"
health:
`+healthBlock+`
`+validBackend+`
shards:
  - id: shard-0
    primary: "127.0.0.1:6543"
`)
			if _, err := Load(path); err == nil {
				t.Fatal("expected an error for incoherent health thresholds")
			}
		})
	}
}

func TestLoadRejectsNegativeDefaultMaxLag(t *testing.T) {
	path := writeConfig(t, `
listen_addr: ":5433"
health_check_interval: "2s"
routing:
  default_max_lag_ms: -1
`+validBackend+`
shards:
  - id: shard-0
    primary: "127.0.0.1:6543"
`)
	if _, err := Load(path); err == nil {
		t.Fatal("expected an error for a negative default_max_lag_ms")
	}
}

// validBackend is a valid `backend:` block to append to fixtures that
// are testing something else, so those tests actually exercise the
// validation rule they claim to — not fail earlier for an unrelated
// missing-backend reason.
const validBackend = `
backend:
  user: postgres
  database: postgres
`

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
`+validBackend+`
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
`+validBackend+`
shards: []
`)
	if _, err := Load(path); err == nil {
		t.Fatal("expected an error when no shards are defined")
	}
}

func TestLoadMissingBackendUser(t *testing.T) {
	path := writeConfig(t, `
listen_addr: ":5433"
health_check_interval: "2s"
backend:
  database: postgres
shards:
  - id: shard-0
    primary: "127.0.0.1:6543"
`)
	if _, err := Load(path); err == nil {
		t.Fatal("expected an error when backend.user is missing")
	}
}

func TestLoadMissingBackendDatabase(t *testing.T) {
	path := writeConfig(t, `
listen_addr: ":5433"
health_check_interval: "2s"
backend:
  user: postgres
shards:
  - id: shard-0
    primary: "127.0.0.1:6543"
`)
	if _, err := Load(path); err == nil {
		t.Fatal("expected an error when backend.database is missing")
	}
}

func TestLoadDuplicateShardID(t *testing.T) {
	path := writeConfig(t, `
listen_addr: ":5433"
health_check_interval: "2s"
`+validBackend+`
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
`+validBackend+`
shards:
  - id: shard-0
`)
	if _, err := Load(path); err == nil {
		t.Fatal("expected an error for a shard with no primary")
	}
}
