package health

import "dbfabric/internal/shardmap"

// Controller promotes a replica and atomically updates the shard map
// when a primary is confirmed DOWN (after N consecutive missed
// heartbeats, to avoid flapping on a single blip).
//
// TODO:
//   - Promote: internal election among replicas, or a call out to an
//     external orchestrator (Patroni-style) that already owns this job.
//   - Split-brain guard: before promoting, confirm via a quorum check
//     (do a majority of replicas also no longer reach the old primary?)
//     rather than trusting the checker's own network view alone — the
//     proxy being partitioned from the primary doesn't mean the primary
//     is actually down for clients.
//   - On promotion: demote the old primary to "unreachable" (not
//     silently reused even if it comes back, until confirmed rejoined
//     as a replica) and signal the pool manager to drain its pool.
type Controller struct {
	shardMap *shardmap.Map
}

func NewController(sm *shardmap.Map) *Controller {
	return &Controller{shardMap: sm}
}

// Promote handles a confirmed-DOWN primary for the given shard.
func (c *Controller) Promote(shardID string) error {
	panic("not implemented")
}
