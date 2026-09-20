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

	"dbfabric/internal/pool"
	"dbfabric/internal/shardmap"
)

const (
	defaultInterval           = 2 * time.Second
	defaultSuspectAfterMisses = 1
	defaultDownAfterMisses    = 3
	defaultPromoteTimeout     = 30 * time.Second
)

// Options configures a Checker. Zero values for the numeric fields fall
// back to defaults; the booleans are literal (false means off).
type Options struct {
	// Interval between heartbeat sweeps. Each node's ping is bounded by
	// half of it, so one wedged node can't stall a sweep.
	Interval time.Duration
	// SuspectAfterMisses / DownAfterMisses: consecutive missed heartbeats
	// before a node is SUSPECT / DOWN. Only DOWN triggers failover.
	SuspectAfterMisses int
	DownAfterMisses    int
	// PgPromote issues pg_promote() on the replica chosen at failover.
	PgPromote bool
	// QuorumGuard makes failover consult the replicas first and refuse to
	// promote unless a majority of them also cannot see the primary.
	QuorumGuard bool
	// PromoteTimeout bounds one whole failover attempt (guard vote plus
	// pg_promote).
	PromoteTimeout time.Duration
}

func (o Options) withDefaults() Options {
	if o.Interval <= 0 {
		o.Interval = defaultInterval
	}
	if o.SuspectAfterMisses <= 0 {
		o.SuspectAfterMisses = defaultSuspectAfterMisses
	}
	if o.DownAfterMisses <= 0 {
		o.DownAfterMisses = defaultDownAfterMisses
	}
	if o.PromoteTimeout <= 0 {
		o.PromoteTimeout = defaultPromoteTimeout
	}
	return o
}

// Checker heartbeats every node in the shard map on an interval,
// updates each node's Status/LagMS, and drives failover for any shard
// whose primary is DOWN.
//
// Concurrency model: shards are checked independently and in parallel,
// and within a shard every node is pinged in parallel. At most one check
// runs per shard at a time — a shard whose previous check is still
// running (typically a slow failover) is skipped by later sweeps rather
// than overlapped. That guard is also what makes it safe for a check to
// replace a shard's entry in the shard map: nothing else writes that
// shard concurrently.
type Checker struct {
	shardMap *shardmap.Map
	ops      backends
	failover *Controller
	opts     Options

	mu              sync.Mutex
	misses          map[string]int    // node addr -> consecutive missed heartbeats
	busy            map[string]bool   // shard ID -> a check is in flight
	lastFailoverErr map[string]string // shard ID -> last logged failover error
}

// NewChecker builds a Checker that talks to real backends through pm.
func NewChecker(sm *shardmap.Map, pm *pool.Manager, opts Options) *Checker {
	return newCheckerWithBackends(sm, pgxBackends{pools: pm}, opts)
}

func newCheckerWithBackends(sm *shardmap.Map, ops backends, opts Options) *Checker {
	opts = opts.withDefaults()
	return &Checker{
		shardMap:        sm,
		ops:             ops,
		failover:        newController(sm, ops, opts.PgPromote, opts.QuorumGuard),
		opts:            opts,
		misses:          make(map[string]int),
		busy:            make(map[string]bool),
		lastFailoverErr: make(map[string]string),
	}
}

// Run sweeps once immediately (so status and lag are populated right
// away) and then on every tick, until ctx is cancelled. Each sweep runs in
// its own goroutine so a slow one never delays the next tick; Run waits
// for in-flight sweeps before returning.
func (c *Checker) Run(ctx context.Context) error {
	ticker := time.NewTicker(c.opts.Interval)
	defer ticker.Stop()

	var inflight sync.WaitGroup
	defer inflight.Wait()
	launch := func() {
		inflight.Add(1)
		go func() {
			defer inflight.Done()
			c.sweep(ctx)
		}()
	}

	launch()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			launch()
		}
	}
}

// sweep checks every shard once, in parallel, and returns when all of
// them (skipping any whose previous check is still running) are done.
func (c *Checker) sweep(ctx context.Context) {
	var wg sync.WaitGroup
	for _, shard := range c.shardMap.All() {
		if !c.tryAcquire(shard.ID) {
			continue
		}
		wg.Add(1)
		go func(s *shardmap.Shard) {
			defer wg.Done()
			defer c.release(s.ID)
			c.checkShard(ctx, s)
		}(shard)
	}
	wg.Wait()
}

func (c *Checker) tryAcquire(shardID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.busy[shardID] {
		return false
	}
	c.busy[shardID] = true
	return true
}

func (c *Checker) release(shardID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.busy, shardID)
}

// checkShard pings the primary and every replica of one shard in
// parallel, writes back a full replacement Shard with fresh Status/LagMS,
// and — if the primary is DOWN — attempts failover.
//
// It must never mutate old or the Nodes inside it: readers hold those
// pointers via shardmap.Map.Get, so updates go in as a whole new Shard
// (see shardmap.Map.Set).
func (c *Checker) checkShard(ctx context.Context, old *shardmap.Shard) {
	var wg sync.WaitGroup
	var newPrimary shardmap.Node
	newReplicas := make([]shardmap.Node, len(old.Replicas))

	wg.Add(1 + len(old.Replicas))
	go func() {
		defer wg.Done()
		newPrimary = c.pingNode(ctx, old.Primary, false)
	}()
	for i, r := range old.Replicas {
		go func(i int, r shardmap.Node) {
			defer wg.Done()
			newReplicas[i] = c.pingNode(ctx, r, true)
		}(i, r)
	}
	wg.Wait()

	c.shardMap.Set(old.ID, &shardmap.Shard{ID: old.ID, Primary: newPrimary, Replicas: newReplicas})

	if newPrimary.Status != shardmap.StatusDown {
		c.clearFailoverErr(old.ID)
		return
	}
	if old.Primary.Status != shardmap.StatusDown {
		log.Printf("health: shard %q primary %s confirmed DOWN after %d missed heartbeats",
			old.ID, old.Primary.Addr, c.opts.DownAfterMisses)
	}
	c.attemptFailover(ctx, old.ID)
}

// attemptFailover runs on every sweep while a shard's primary is DOWN, not
// just at the moment it goes down: a promotion the guard vetoed (or that
// found no healthy replica yet) must be retried once conditions change. To
// keep a persistent failure from logging every sweep, an error is logged
// only when it differs from the last one logged for that shard.
func (c *Checker) attemptFailover(ctx context.Context, shardID string) {
	pctx, cancel := context.WithTimeout(ctx, c.opts.PromoteTimeout)
	defer cancel()

	err := c.failover.Promote(pctx, shardID)
	if err == nil {
		c.clearFailoverErr(shardID)
		return
	}

	msg := err.Error()
	c.mu.Lock()
	seen := c.lastFailoverErr[shardID] == msg
	c.lastFailoverErr[shardID] = msg
	c.mu.Unlock()
	if !seen {
		log.Printf("health: failover for shard %q not performed: %v", shardID, err)
	}
}

func (c *Checker) clearFailoverErr(shardID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.lastFailoverErr, shardID)
}

// pingNode checks one node's liveness (and, for replicas, its
// replication lag) with a timeout of half the sweep interval. It returns
// a full replacement Node — callers must Set it back via the shard map
// rather than mutating the node they had, per shardmap.Map.Set's contract.
func (c *Checker) pingNode(ctx context.Context, n shardmap.Node, isReplica bool) shardmap.Node {
	pingCtx, cancel := context.WithTimeout(ctx, c.opts.Interval/2)
	defer cancel()

	lagMS := n.LagMS
	var err error
	if isReplica {
		var fresh int64
		if fresh, err = c.ops.ReplicationLagMS(pingCtx, n.Addr); err == nil {
			lagMS = fresh
		}
	} else {
		err = c.ops.Ping(pingCtx, n.Addr)
	}
	// On failure lagMS stays at the node's last known value rather than
	// resetting to 0, which would make a dead replica look perfectly
	// fresh to bounded-staleness routing. Status still reflects the miss.

	misses := c.recordResult(n.Addr, err == nil)
	status := shardmap.StatusUp
	switch {
	case misses >= c.opts.DownAfterMisses:
		status = shardmap.StatusDown
	case misses >= c.opts.SuspectAfterMisses:
		status = shardmap.StatusSuspect
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
