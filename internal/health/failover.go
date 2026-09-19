package health

import (
	"fmt"
	"log"

	"dbfabric/internal/pool"
	"dbfabric/internal/shardmap"
)

// Controller promotes a replica and atomically updates the shard map
// when the health checker has confirmed a shard's primary DOWN (after
// N consecutive missed heartbeats, so a single blip doesn't trigger
// this).
//
// TODO:
//   - Split-brain guard: before promoting, confirm via a quorum check
//     (do a majority of replicas also no longer reach the old primary?)
//     rather than trusting this checker's own network view alone — the
//     proxy being partitioned from the primary doesn't mean the primary
//     is actually down for clients. Not implemented: this is a single
//     proxy instance today (see README's "known limitations" section),
//     so there's no second vantage point to quorum against yet.
//   - This updates routing state only. It does not perform a real
//     PostgreSQL promotion (issuing pg_promote() on the chosen
//     replica, or calling out to an external orchestrator like
//     Patroni) — the "new primary" is still a read replica at the
//     database level until something does that separately.
type Controller struct {
	shardMap *shardmap.Map
	pools    *pool.Manager
}

func NewController(sm *shardmap.Map, pm *pool.Manager) *Controller {
	return &Controller{shardMap: sm, pools: pm}
}

// Promote replaces shardID's primary with its least-lagged non-DOWN
// replica (minimizing how much data the promoted node is missing),
// drains the old primary's connection pool so in-flight/new queries
// against it fail fast rather than hang or get silently retried, and
// updates the shard map so the router picks up the new primary on its
// very next Resolve call for this shard.
func (c *Controller) Promote(shardID string) error {
	shard, ok := c.shardMap.Get(shardID)
	if !ok {
		return fmt.Errorf("health: cannot promote unknown shard %q", shardID)
	}

	newPrimary, remainingReplicas, ok := selectPromotionCandidate(shard.Replicas)
	if !ok {
		return fmt.Errorf("health: shard %q has no healthy replica to promote", shardID)
	}
	oldPrimaryAddr := shard.Primary.Addr

	c.shardMap.Set(shardID, &shardmap.Shard{
		ID:       shard.ID,
		Primary:  shardmap.Node{Addr: newPrimary.Addr, Status: shardmap.StatusUp},
		Replicas: remainingReplicas,
	})
	c.pools.Drain(oldPrimaryAddr)

	log.Printf("health: shard %q failed over: primary %s -> %s (old primary's pool drained)",
		shardID, oldPrimaryAddr, newPrimary.Addr)
	return nil
}

// selectPromotionCandidate picks the least-lagged non-DOWN replica to
// promote, and returns the remaining replica set with it removed. ok
// is false if no replica qualifies (all DOWN, or none exist) — the
// caller should leave the shard as-is rather than promote nothing.
func selectPromotionCandidate(replicas []shardmap.Node) (candidate shardmap.Node, remaining []shardmap.Node, ok bool) {
	best := -1
	for i, r := range replicas {
		if r.Status == shardmap.StatusDown {
			continue
		}
		if best == -1 || r.LagMS < replicas[best].LagMS {
			best = i
		}
	}
	if best == -1 {
		return shardmap.Node{}, nil, false
	}

	remaining = make([]shardmap.Node, 0, len(replicas)-1)
	for i, r := range replicas {
		if i != best {
			remaining = append(remaining, r)
		}
	}
	return replicas[best], remaining, true
}
