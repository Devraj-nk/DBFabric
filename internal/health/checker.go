// Package health heartbeats every backend node, maintains the
// UP/SUSPECT/DOWN + replication-lag status the router reads for
// bounded-staleness routing, and drives failover when a primary is
// confirmed down.
package health

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"dbfabric/internal/pool"
	"dbfabric/internal/shardmap"
)

const (
	// suspectAfterMisses/downAfterMisses are not yet configurable —
	// see the TODO on Checker. One miss is enough to go SUSPECT
	// (visible to bounded-staleness routing, cheap to reconsider next
	// tick); three consecutive misses before DOWN (and therefore
	// before failover) avoids triggering on a single blip.
	suspectAfterMisses = 1
	downAfterMisses    = 3
)

// Checker heartbeats every node in the shard map on an interval,
// updates each node's Status/LagMS, and triggers failover the moment
// a primary's status transitions to DOWN.
//
// TODO: node pings within a sweep run sequentially, not fanned out
// across goroutines as the original design sketch called for (see
// README's health-check data flow) — fine while shard/node counts are
// small relative to the check interval, worth revisiting if that
// stops being true. Miss thresholds above are also fixed constants,
// not sourced from config yet.
type Checker struct {
	shardMap *shardmap.Map
	pools    *pool.Manager
	failover *Controller
	interval time.Duration

	mu     sync.Mutex
	misses map[string]int // node addr -> consecutive missed heartbeats
}

func NewChecker(sm *shardmap.Map, pm *pool.Manager, interval time.Duration) *Checker {
	return &Checker{
		shardMap: sm,
		pools:    pm,
		failover: NewController(sm, pm),
		interval: interval,
		misses:   make(map[string]int),
	}
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
			c.sweep(ctx)
		}
	}
}

// sweep pings every node of every shard once and writes back updated
// status/lag, then triggers failover for any shard whose primary just
// transitioned to DOWN this sweep.
func (c *Checker) sweep(ctx context.Context) {
	for _, shard := range c.shardMap.All() {
		c.checkShard(ctx, shard)
	}
}

func (c *Checker) checkShard(ctx context.Context, old *shardmap.Shard) {
	newPrimary := c.pingNode(ctx, old.Primary, false)
	newReplicas := make([]shardmap.Node, len(old.Replicas))
	for i, r := range old.Replicas {
		newReplicas[i] = c.pingNode(ctx, r, true)
	}

	c.shardMap.Set(old.ID, &shardmap.Shard{ID: old.ID, Primary: newPrimary, Replicas: newReplicas})

	primaryJustDied := newPrimary.Status == shardmap.StatusDown && old.Primary.Status != shardmap.StatusDown
	if !primaryJustDied {
		return
	}
	log.Printf("health: shard %q primary %s confirmed DOWN after %d misses, promoting a replica",
		old.ID, old.Primary.Addr, downAfterMisses)
	if err := c.failover.Promote(old.ID); err != nil {
		log.Printf("health: failover for shard %q failed: %v", old.ID, err)
	}
}

// pingNode checks one node's liveness (and, for replicas, its
// replication lag) with a timeout bounded well under the check
// interval, so one wedged node can't stall the rest of the sweep. It
// returns a full replacement Node — callers must Set it back via the
// shard map rather than mutating the node they had, per
// shardmap.Map.Set's contract.
func (c *Checker) pingNode(ctx context.Context, n shardmap.Node, isReplica bool) shardmap.Node {
	pingCtx, cancel := context.WithTimeout(ctx, c.interval/2)
	defer cancel()

	var lagMS int64
	ok := false
	if backend, err := c.pools.Get(pingCtx, n.Addr); err == nil {
		if isReplica {
			lagMS, ok = replicationLagMS(pingCtx, backend)
		} else {
			ok = ping(pingCtx, backend)
		}
	}

	misses := c.recordResult(n.Addr, ok)
	status := shardmap.StatusUp
	switch {
	case misses >= downAfterMisses:
		status = shardmap.StatusDown
	case misses >= suspectAfterMisses:
		status = shardmap.StatusSuspect
	}

	// A failed ping means lagMS wasn't actually refreshed; keep
	// reporting the node's last known lag rather than resetting it to
	// 0, which would make a stale/dead replica look perfectly fresh to
	// bounded-staleness routing. Status still reflects the failure.
	if !ok {
		lagMS = n.LagMS
	}

	return shardmap.Node{Addr: n.Addr, Status: status, LagMS: lagMS}
}

func (c *Checker) recordResult(addr string, ok bool) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ok {
		delete(c.misses, addr)
		return 0
	}
	c.misses[addr]++
	return c.misses[addr]
}

// ping is a bare liveness check for a primary (or a replica beyond
// just its lag).
func ping(ctx context.Context, backend *pgxpool.Pool) bool {
	var one int
	return backend.QueryRow(ctx, "SELECT 1").Scan(&one) == nil
}

// replicationLagMS asks a replica how far behind the primary's WAL it
// currently is. pg_last_xact_replay_timestamp() is NULL when the
// server isn't actually in recovery (e.g. it's a plain standalone
// instance, not a real streaming replica) — COALESCE to 0 in that
// case rather than erroring, since a successful round trip already
// proves the node is alive even if it can't report a meaningful lag.
func replicationLagMS(ctx context.Context, backend *pgxpool.Pool) (int64, bool) {
	var lagMS float64
	err := backend.QueryRow(ctx,
		`SELECT COALESCE(EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp())), 0) * 1000`,
	).Scan(&lagMS)
	if err != nil {
		return 0, false
	}
	return int64(lagMS), true
}
