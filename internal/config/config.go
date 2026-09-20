// Package config loads the proxy's static configuration: listen
// address, initial shard map, health-check behavior, routing defaults.
// See config/shards.example.yaml for the shape this parses.
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Defaults applied when an optional field is omitted from the YAML.
const (
	DefaultSuspectAfterMisses = 1
	DefaultDownAfterMisses    = 3
	DefaultMaxLagMS           = 1000
)

type Config struct {
	ListenAddr          string
	HealthCheckInterval time.Duration
	Health              HealthConfig
	Routing             RoutingConfig
	Backend             BackendConfig
	Shards              []ShardConfig
}

type ShardConfig struct {
	ID       string
	Primary  string
	Replicas []string
}

// HealthConfig tunes failure detection and failover. Every field has a
// default, so the whole `health:` block is optional.
type HealthConfig struct {
	// SuspectAfterMisses / DownAfterMisses are consecutive missed
	// heartbeats before a node is marked SUSPECT / DOWN. Only DOWN
	// triggers failover, so DownAfterMisses is the flap-suppression knob.
	SuspectAfterMisses int
	DownAfterMisses    int
	// PgPromote makes failover issue pg_promote() on the chosen replica
	// before rerouting to it. Turn it off only if something else (an
	// orchestrator) promotes the replica.
	PgPromote bool
	// QuorumGuard makes failover ask the replicas whether they can still
	// see the primary, and refuse to promote unless a majority cannot.
	QuorumGuard bool
}

// RoutingConfig holds query-routing defaults.
type RoutingConfig struct {
	// DefaultMaxLagMS is the bounded-staleness lag ceiling used when a
	// query's consistency hint doesn't carry its own.
	DefaultMaxLagMS int64
}

// BackendConfig is the credentials used to connect to every backend
// node. The proxy assumes one uniform application role/database
// across the shard fleet — see internal/pool.Backend, which this maps
// onto directly.
type BackendConfig struct {
	User     string
	Password string
	Database string
	SSLMode  string
}

// rawConfig mirrors the YAML shape directly. HealthCheckInterval stays
// a string here (e.g. "2s") since yaml.v3 can't unmarshal a duration
// string straight into a time.Duration; Load parses it afterward.
// Optional numeric/bool fields are pointers so "omitted" (use the
// default) is distinguishable from an explicit zero/false.
type rawConfig struct {
	ListenAddr          string           `yaml:"listen_addr"`
	HealthCheckInterval string           `yaml:"health_check_interval"`
	Health              rawHealthConfig  `yaml:"health"`
	Routing             rawRoutingConfig `yaml:"routing"`
	Backend             rawBackendConfig `yaml:"backend"`
	Shards              []rawShardConfig `yaml:"shards"`
}

type rawHealthConfig struct {
	SuspectAfterMisses *int  `yaml:"suspect_after_misses"`
	DownAfterMisses    *int  `yaml:"down_after_misses"`
	PgPromote          *bool `yaml:"pg_promote"`
	QuorumGuard        *bool `yaml:"quorum_guard"`
}

type rawRoutingConfig struct {
	DefaultMaxLagMS *int64 `yaml:"default_max_lag_ms"`
}

type rawBackendConfig struct {
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
	SSLMode  string `yaml:"sslmode"`
}

type rawShardConfig struct {
	ID       string   `yaml:"id"`
	Primary  string   `yaml:"primary"`
	Replicas []string `yaml:"replicas"`
}

// Load reads and parses a shard-map config file (see
// config/shards.example.yaml) and validates it: every shard needs a
// unique id and a primary address, and health thresholds must be
// coherent.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: reading %s: %w", path, err)
	}

	var raw rawConfig
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("config: parsing %s: %w", path, err)
	}

	interval, err := time.ParseDuration(raw.HealthCheckInterval)
	if err != nil {
		return nil, fmt.Errorf("config: invalid health_check_interval %q: %w", raw.HealthCheckInterval, err)
	}

	health, err := resolveHealth(raw.Health)
	if err != nil {
		return nil, err
	}
	routing, err := resolveRouting(raw.Routing)
	if err != nil {
		return nil, err
	}

	if len(raw.Shards) == 0 {
		return nil, fmt.Errorf("config: %s defines no shards", path)
	}
	if raw.Backend.User == "" {
		return nil, fmt.Errorf("config: backend.user is required")
	}
	if raw.Backend.Database == "" {
		return nil, fmt.Errorf("config: backend.database is required")
	}

	cfg := &Config{
		ListenAddr:          raw.ListenAddr,
		HealthCheckInterval: interval,
		Health:              health,
		Routing:             routing,
		Backend: BackendConfig{
			User:     raw.Backend.User,
			Password: raw.Backend.Password,
			Database: raw.Backend.Database,
			SSLMode:  raw.Backend.SSLMode,
		},
		Shards: make([]ShardConfig, len(raw.Shards)),
	}

	seen := make(map[string]bool, len(raw.Shards))
	for i, s := range raw.Shards {
		if s.ID == "" {
			return nil, fmt.Errorf("config: shard at index %d has no id", i)
		}
		if seen[s.ID] {
			return nil, fmt.Errorf("config: duplicate shard id %q", s.ID)
		}
		seen[s.ID] = true
		if s.Primary == "" {
			return nil, fmt.Errorf("config: shard %q has no primary", s.ID)
		}
		cfg.Shards[i] = ShardConfig{ID: s.ID, Primary: s.Primary, Replicas: s.Replicas}
	}

	return cfg, nil
}

func resolveHealth(raw rawHealthConfig) (HealthConfig, error) {
	h := HealthConfig{
		SuspectAfterMisses: DefaultSuspectAfterMisses,
		DownAfterMisses:    DefaultDownAfterMisses,
		PgPromote:          true,
		QuorumGuard:        true,
	}
	if raw.SuspectAfterMisses != nil {
		h.SuspectAfterMisses = *raw.SuspectAfterMisses
	}
	if raw.DownAfterMisses != nil {
		h.DownAfterMisses = *raw.DownAfterMisses
	}
	if raw.PgPromote != nil {
		h.PgPromote = *raw.PgPromote
	}
	if raw.QuorumGuard != nil {
		h.QuorumGuard = *raw.QuorumGuard
	}

	if h.SuspectAfterMisses < 1 {
		return HealthConfig{}, fmt.Errorf("config: health.suspect_after_misses must be >= 1, got %d", h.SuspectAfterMisses)
	}
	if h.DownAfterMisses < h.SuspectAfterMisses {
		return HealthConfig{}, fmt.Errorf("config: health.down_after_misses (%d) must be >= health.suspect_after_misses (%d)",
			h.DownAfterMisses, h.SuspectAfterMisses)
	}
	return h, nil
}

func resolveRouting(raw rawRoutingConfig) (RoutingConfig, error) {
	r := RoutingConfig{DefaultMaxLagMS: DefaultMaxLagMS}
	if raw.DefaultMaxLagMS != nil {
		r.DefaultMaxLagMS = *raw.DefaultMaxLagMS
	}
	if r.DefaultMaxLagMS < 0 {
		return RoutingConfig{}, fmt.Errorf("config: routing.default_max_lag_ms must be >= 0, got %d", r.DefaultMaxLagMS)
	}
	return r, nil
}
