// Package config loads the proxy's static configuration: listen
// address, initial shard map, health-check interval. See
// config/shards.example.yaml for the shape this parses.
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	ListenAddr          string
	HealthCheckInterval time.Duration
	Backend             BackendConfig
	Shards              []ShardConfig
}

type ShardConfig struct {
	ID       string
	Primary  string
	Replicas []string
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
type rawConfig struct {
	ListenAddr          string           `yaml:"listen_addr"`
	HealthCheckInterval string           `yaml:"health_check_interval"`
	Backend             rawBackendConfig `yaml:"backend"`
	Shards              []rawShardConfig `yaml:"shards"`
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
// unique id and a primary address.
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
