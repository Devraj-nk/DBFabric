package health

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"dbfabric/internal/shardmap"
)

const (
	// pgPromoteWait is how long pg_promote() is told to wait for the
	// standby to finish promotion. It must stay comfortably under the
	// overall promote timeout the checker applies.
	pgPromoteWait = 20 * time.Second
	// witnessTimeout bounds each replica's answer to "can you still see
	// the primary?", so one wedged replica can't stall the vote.
	witnessTimeout = 3 * time.Second
)

// ErrPromotionVetoed is returned (wrapped) when the split-brain guard
// refuses to promote because a majority of reachable replicas can still
// see the primary.
var ErrPromotionVetoed = errors.New("promotion vetoed by split-brain guard")

// Controller promotes a replica and atomically updates the shard map
// when the health checker has confirmed a shard's primary DOWN (after
// N consecutive missed heartbeats, so a single blip doesn't trigger
// this).
//
// Ordering matters and is deliberate:
//  1. Split-brain guard (if enabled): ask the replicas whether they can
//     still see the primary. The checker's own failed heartbeats only
//     prove the *proxy* can't reach the primary; if the replicas are
//     still streaming from it, the primary is alive for everyone else
//     and promoting would create two writable primaries.
//  2. pg_promote() (if enabled) on the chosen replica, waiting for it to
//     finish. If this fails the shard map is left untouched — we never
//     route writes at a node that can't accept them.
//  3. Only then swap the shard map and drain the old primary's pool.
//
// Known limits, none of which this controller attempts:
//   - The guard uses the replicas as witnesses because there is a single
//     proxy instance today (see README's "known limitations"). It relies
//     on replicas noticing a dead primary themselves, which for a silent
//     network partition takes up to wal_receiver_timeout (60s by
//     default) — the guard errs toward not promoting during that window.
//   - The old primary is not fenced. If it is alive but unreachable from
//     the proxy, nothing stops other clients that can reach it from still
//     writing to it after promotion.
//   - The old primary is not re-added as a replica when it returns, and
//     the surviving replicas are not repointed at the new primary.
type Controller struct {
	shardMap    *shardmap.Map
	ops         backends
	pgPromote   bool
	quorumGuard bool
}

func newController(sm *shardmap.Map, ops backends, pgPromote, quorumGuard bool) *Controller {
	return &Controller{shardMap: sm, ops: ops, pgPromote: pgPromote, quorumGuard: quorumGuard}
}

// Promote replaces shardID's primary with its least-lagged non-DOWN
// replica, subject to the ordering above. On any error the shard map is
// left exactly as it was.
func (c *Controller) Promote(ctx context.Context, shardID string) error {
	shard, ok := c.shardMap.Get(shardID)
	if !ok {
		return fmt.Errorf("health: cannot promote unknown shard %q", shardID)
	}

	newPrimary, remainingReplicas, ok := selectPromotionCandidate(shard.Replicas)
	if !ok {
		return fmt.Errorf("health: shard %q has no healthy replica to promote", shardID)
	}
	oldPrimaryAddr := shard.Primary.Addr

	if c.quorumGuard {
		if err := c.checkQuorum(ctx, shard); err != nil {
			return err
		}
	}
	if c.pgPromote {
		if err := c.ops.PgPromote(ctx, newPrimary.Addr, pgPromoteWait); err != nil {
			return fmt.Errorf("health: pg_promote on %s for shard %q: %w", newPrimary.Addr, shardID, err)
		}
	}

	c.shardMap.Set(shardID, &shardmap.Shard{
		ID:       shard.ID,
		Primary:  shardmap.Node{Addr: newPrimary.Addr, Status: shardmap.StatusUp},
		Replicas: remainingReplicas,
	})
	c.ops.Drain(oldPrimaryAddr)

	log.Printf("health: shard %q failed over: primary %s -> %s (old primary's pool drained)",
		shardID, oldPrimaryAddr, newPrimary.Addr)
	return nil
}

// checkQuorum polls every non-DOWN replica for whether its WAL receiver is
// still streaming from the primary, and returns ErrPromotionVetoed unless
// a strict majority of the replicas that answered cannot see it.
func (c *Controller) checkQuorum(ctx context.Context, shard *shardmap.Shard) error {
	var witnesses []shardmap.Node
	for _, r := range shard.Replicas {
		if r.Status != shardmap.StatusDown {
			witnesses = append(witnesses, r)
		}
	}

	type vote struct {
		streaming bool
		err       error
	}
	votes := make([]vote, len(witnesses))
	var wg sync.WaitGroup
	for i, w := range witnesses {
		wg.Add(1)
		go func(i int, addr string) {
			defer wg.Done()
			wctx, cancel := context.WithTimeout(ctx, witnessTimeout)
			defer cancel()
			streaming, err := c.ops.WalReceiverStreaming(wctx, addr)
			votes[i] = vote{streaming: streaming, err: err}
		}(i, w.Addr)
	}
	wg.Wait()

	var sawPrimary, cannotSee int
	for _, v := range votes {
		switch {
		case v.err != nil:
			// A witness we can't reach abstains; it neither confirms nor
			// denies that the primary is alive.
		case v.streaming:
			sawPrimary++
		default:
			cannotSee++
		}
	}
	if !quorumAllowsPromotion(sawPrimary, cannotSee) {
		return fmt.Errorf("%w: %d replica(s) still see primary %s, %d cannot (%d abstained)",
			ErrPromotionVetoed, sawPrimary, shard.Primary.Addr, cannotSee, len(witnesses)-sawPrimary-cannotSee)
	}
	return nil
}

// quorumAllowsPromotion requires at least one witness to have answered
// and a strict majority of the answering witnesses to be unable to see
// the primary. A tie vetoes: when unsure, don't create a second primary.
func quorumAllowsPromotion(sawPrimary, cannotSee int) bool {
	reachable := sawPrimary + cannotSee
	return reachable > 0 && cannotSee*2 > reachable
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
