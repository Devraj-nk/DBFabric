// Package router extracts a shard key from an incoming query/session,
// resolves it to a shard via consistent hashing, and picks a target
// node (primary or replica) based on the requested consistency level.
package router

import (
	"fmt"

	"dbfabric/internal/shardmap"
)

// Consistency is the caller's requested read consistency level.
type Consistency string

const (
	Strong           Consistency = "strong"           // read from primary
	Eventual         Consistency = "eventual"          // read from any (healthy) replica
	BoundedStaleness Consistency = "bounded_staleness" // read from a replica only if lag <= threshold
)

const defaultVnodesPerShard = 100

// Router resolves (shard key, consistency) -> target node.
//
// It owns the consistent-hash ring itself; the shard map (primary /
// replica / lag data per shard) is a separate object so the health
// checker and failover controller can update a shard's primary
// in-place (shardMap.Set) without touching ring topology — the ring
// should only change when a shard is actually added or removed.
type Router struct {
	shardMap *shardmap.Map
	ring     *Ring
}

func New(sm *shardmap.Map) *Router {
	return &Router{shardMap: sm, ring: NewRing(defaultVnodesPerShard)}
}

// AddShard registers a shard's topology: adds it to both the shard
// map and the hash ring. Use this for initial load and for actually
// adding/removing shards — not for updating an existing shard's
// primary after a failover (call shardMap.Set directly for that, so
// every other key's routing is undisturbed).
func (r *Router) AddShard(s *shardmap.Shard) {
	r.shardMap.Set(s.ID, s)
	r.ring.AddShard(s.ID)
}

// Resolve picks the node that should handle a query for shardKey
// under the given consistency requirement. maxLagMS is only
// consulted for BoundedStaleness; pass 0 for the other levels.
//
// A replica with Status == shardmap.StatusDown (as last reported by
// internal/health) is never chosen — an unset Status (the zero value,
// before the health checker's first sweep, or in tests that don't set
// it) is treated as usable, not as known-bad.
func (r *Router) Resolve(shardKey string, c Consistency, maxLagMS int64) (shardmap.Node, error) {
	shardID, ok := r.ring.ShardFor(shardKey)
	if !ok {
		return shardmap.Node{}, fmt.Errorf("router: no shards registered")
	}
	shard, ok := r.shardMap.Get(shardID)
	if !ok {
		return shardmap.Node{}, fmt.Errorf("router: shard %q is on the ring but missing from the shard map", shardID)
	}

	switch c {
	case Strong, "":
		return shard.Primary, nil
	case Eventual:
		return pickReplica(shard, shardKey, -1), nil // -1: no lag ceiling, health is the only filter
	case BoundedStaleness:
		return pickReplica(shard, shardKey, maxLagMS), nil
	default:
		return shardmap.Node{}, fmt.Errorf("router: unknown consistency level %q", c)
	}
}

// pickReplica deterministically spreads a shard key across the
// shard's usable replicas (hash(key) mod len(candidates)) instead of
// tracking round-robin state per shard. A replica is a candidate if
// it isn't marked DOWN and, when maxLagMS >= 0, its LagMS doesn't
// exceed it. Falls back to primary if no replica qualifies (including
// when the shard has none at all).
func pickReplica(s *shardmap.Shard, shardKey string, maxLagMS int64) shardmap.Node {
	candidates := make([]shardmap.Node, 0, len(s.Replicas))
	for _, r := range s.Replicas {
		if r.Status == shardmap.StatusDown {
			continue
		}
		if maxLagMS >= 0 && r.LagMS > maxLagMS {
			continue
		}
		candidates = append(candidates, r)
	}
	if len(candidates) == 0 {
		return s.Primary
	}
	idx := hashKey(shardKey) % uint32(len(candidates))
	return candidates[idx]
}
