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
	Eventual         Consistency = "eventual"          // read from any replica
	BoundedStaleness Consistency = "bounded_staleness" // read from a replica only if lag < threshold
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
// under the given consistency requirement.
//
// No health checker/failover exists yet (this is build-order step 1:
// static shard map, route by hash, no failover). Bounded-staleness
// falls back to primary for now — it will start consulting replica
// lag once internal/health populates Node.LagMS.
func (r *Router) Resolve(shardKey string, c Consistency) (shardmap.Node, error) {
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
		return pickReplica(shard, shardKey), nil
	case BoundedStaleness:
		// TODO: once internal/health populates Node.LagMS, prefer a
		// replica with LagMS below the caller's threshold and fall
		// back to primary only if none qualify.
		return shard.Primary, nil
	default:
		return shardmap.Node{}, fmt.Errorf("router: unknown consistency level %q", c)
	}
}

// pickReplica deterministically spreads a shard key across its
// replicas (hash(key) mod len(replicas)) instead of tracking
// round-robin state per shard. Falls back to primary if the shard has
// no replicas registered yet.
func pickReplica(s *shardmap.Shard, shardKey string) shardmap.Node {
	if len(s.Replicas) == 0 {
		return s.Primary
	}
	idx := hashKey(shardKey) % uint32(len(s.Replicas))
	return s.Replicas[idx]
}
