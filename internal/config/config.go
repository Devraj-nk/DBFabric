// Package config loads the proxy's static configuration: listen
// address, initial shard map, health-check interval. See
// config/shards.example.yaml for the shape this will parse.
package config

import "time"

type Config struct {
	ListenAddr          string
	HealthCheckInterval time.Duration
	Shards              []ShardConfig
}

type ShardConfig struct {
	ID       string
	Primary  string
	Replicas []string
}

// Load reads and parses a config file at path.
//
// TODO: YAML parsing (gopkg.in/yaml.v3) once that dependency is added.
func Load(path string) (*Config, error) {
	panic("not implemented")
}
