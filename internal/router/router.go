// Package router extracts a shard key from an incoming query/session,
// resolves it to a shard via consistent hashing, and picks a target
// node (primary or replica) based on the requested consistency level.
package router

import "dbfabric/internal/shardmap"

// Consistency is the caller's requested read consistency level.
type Consistency string

const (
	Strong           Consistency = "strong"           // read from primary
	Eventual         Consistency = "eventual"          // read from any replica
	BoundedStaleness Consistency = "bounded_staleness" // read from a replica only if lag < threshold
)

// Router resolves (shard key, consistency) -> target node.
type Router struct {
	shardMap *shardmap.Map
}

func New(sm *shardmap.Map) *Router {
	return &Router{shardMap: sm}
}

// Resolve picks the node that should handle a query for the given
// shard key under the given consistency requirement.
//
// TODO:
//   - writes always resolve to the shard's primary regardless of consistency
//   - Eventual: round-robin/least-conn across the shard's replicas
//   - BoundedStaleness: pick a replica with LagMS < maxLagMS, else fall back to primary
func (r *Router) Resolve(shardKey string, c Consistency) (shardmap.Node, error) {
	panic("not implemented")
}
