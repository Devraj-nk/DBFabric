// Package shardmap holds the proxy's routing table: which physical
// nodes own each shard, and their current health/lag as last reported
// by the health checker.
package shardmap

import "sync"

// NodeStatus is the last-known health of a backend node, as tracked
// by the health checker (internal/health), not checked live per-request.
type NodeStatus string

const (
	StatusUp      NodeStatus = "UP"
	StatusSuspect NodeStatus = "SUSPECT"
	StatusDown    NodeStatus = "DOWN"
)

// Node is a single Postgres backend (primary or replica).
type Node struct {
	Addr   string
	Status NodeStatus
	LagMS  int64
}

// Shard is one partition's primary + replica set.
type Shard struct {
	ID       string
	Primary  Node
	Replicas []Node
}

// Map is the shard routing table: shard ID -> Shard.
//
// v1 is in-memory, guarded by a single RWMutex — reads (the hot path,
// once routing is implemented) don't block each other; writes (shard
// additions, failover promotions) take the exclusive lock. Promotable
// to an external store (etcd/Consul) later if the proxy itself needs
// to run as more than one instance.
type Map struct {
	mu     sync.RWMutex
	shards map[string]*Shard
}

func New() *Map {
	return &Map{shards: make(map[string]*Shard)}
}

func (m *Map) Get(shardID string) (*Shard, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.shards[shardID]
	return s, ok
}

// Set replaces a shard's routing entry, e.g. on initial load or after
// a failover promotion. Callers must swap in a full *Shard rather than
// mutating one in place, so readers never observe a half-updated shard.
func (m *Map) Set(shardID string, s *Shard) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shards[shardID] = s
}

// All returns a snapshot of every shard currently in the map, for
// callers that need to iterate all of them (the health checker's
// heartbeat sweep). The returned *Shard values are the same ones
// stored in the map — safe to read, but per Set's doc comment, never
// mutate one in place; replace it with Set instead.
func (m *Map) All() []*Shard {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Shard, 0, len(m.shards))
	for _, s := range m.shards {
		out = append(out, s)
	}
	return out
}
