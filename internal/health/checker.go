// Package health heartbeats every backend node, maintains the
// UP/SUSPECT/DOWN + replication-lag status the router reads for
// bounded-staleness routing, and drives failover when a primary is
// confirmed down.
package health

import (
	"context"
	"time"

	"dbfabric/internal/shardmap"
)

// Checker heartbeats every node on an interval.
//
// TODO: one goroutine per node pinging on a ticker (each with its own
// context timeout so a wedged node can't hang the others), fanning
// results into a channel consumed by a single goroutine that owns the
// shard map's node-status table — no lock contention with the read path.
type Checker struct {
	shardMap *shardmap.Map
	interval time.Duration
}

func NewChecker(sm *shardmap.Map, interval time.Duration) *Checker {
	return &Checker{shardMap: sm, interval: interval}
}

// Run starts the heartbeat loop and blocks until ctx is cancelled.
func (c *Checker) Run(ctx context.Context) error {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// TODO: heartbeat each node, update status, hand DOWN
			// primaries to the failover Controller.
		}
	}
}
