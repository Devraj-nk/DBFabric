package health

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"dbfabric/internal/pool"
)

// backends is the set of database-level operations the checker and
// failover controller need. It exists so their logic (miss counting,
// quorum voting, promotion ordering) can be tested hermetically against a
// fake, while production uses pgxBackends over real connection pools.
type backends interface {
	// Ping is a bare liveness check.
	Ping(ctx context.Context, addr string) error
	// ReplicationLagMS reports how far behind its primary a replica's
	// replay is. A node that isn't actually a replica reports 0.
	ReplicationLagMS(ctx context.Context, addr string) (int64, error)
	// WalReceiverStreaming reports whether this node currently has an
	// active WAL receiver streaming from a primary — i.e. whether, from
	// this node's vantage point, its primary is alive and reachable.
	WalReceiverStreaming(ctx context.Context, addr string) (bool, error)
	// PgPromote ensures addr is a writable primary: it issues
	// pg_promote() and waits up to wait for promotion to finish. It is
	// idempotent — a node that is already a primary counts as success.
	PgPromote(ctx context.Context, addr string, wait time.Duration) error
	// Drain discards the connection pool for addr.
	Drain(addr string)
}

// pgxBackends implements backends over the proxy's own connection pools.
//
// The backend role needs enough privilege for the calls below:
// pg_promote() requires superuser (or an explicit EXECUTE grant), and
// pg_stat_wal_receiver's status column is only visible to superusers and
// members of pg_read_all_stats/pg_monitor.
type pgxBackends struct {
	pools *pool.Manager
}

func (b pgxBackends) Ping(ctx context.Context, addr string) error {
	p, err := b.pools.Get(ctx, addr)
	if err != nil {
		return err
	}
	var one int
	return p.QueryRow(ctx, "SELECT 1").Scan(&one)
}

// replicationLagQuery reports replay lag in ms. The CASE matters:
// pg_last_xact_replay_timestamp() is the commit time of the last replayed
// transaction, so on an idle primary now() minus it grows forever even
// though the replica is fully caught up. When the replica has replayed
// everything it has received, it is not lagging, so report 0 instead.
// On a node that isn't in recovery both LSN functions are NULL, the CASE
// falls through to a NULL timestamp difference, and COALESCE yields 0.
const replicationLagQuery = `
SELECT COALESCE(
  CASE WHEN pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn() THEN 0
       ELSE EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))
  END, 0) * 1000`

func (b pgxBackends) ReplicationLagMS(ctx context.Context, addr string) (int64, error) {
	p, err := b.pools.Get(ctx, addr)
	if err != nil {
		return 0, err
	}
	var lagMS float64
	if err := p.QueryRow(ctx, replicationLagQuery).Scan(&lagMS); err != nil {
		return 0, err
	}
	return int64(lagMS), nil
}

func (b pgxBackends) WalReceiverStreaming(ctx context.Context, addr string) (bool, error) {
	p, err := b.pools.Get(ctx, addr)
	if err != nil {
		return false, err
	}
	var streaming bool
	err = p.QueryRow(ctx,
		`SELECT COALESCE((SELECT status = 'streaming' FROM pg_stat_wal_receiver LIMIT 1), false)`,
	).Scan(&streaming)
	return streaming, err
}

// sqlstateObjectNotInPrerequisiteState is what pg_promote() raises on a
// server that isn't in recovery ("recovery is not in progress").
const sqlstateObjectNotInPrerequisiteState = "55000"

func (b pgxBackends) PgPromote(ctx context.Context, addr string, wait time.Duration) error {
	p, err := b.pools.Get(ctx, addr)
	if err != nil {
		return err
	}
	var promoted bool
	err = p.QueryRow(ctx, "SELECT pg_promote(true, $1)", int32(wait.Seconds())).Scan(&promoted)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == sqlstateObjectNotInPrerequisiteState {
			return nil // already a primary
		}
		return err
	}
	if !promoted {
		return fmt.Errorf("pg_promote() on %s did not complete within %s", addr, wait)
	}
	return nil
}

// Drain detaches addr's pool immediately (so nothing new is routed
// through it) but lets the actual close — which waits for in-flight
// queries — finish in the background, so a hung query on a dead primary
// can't hold up the failover that is replacing it.
func (b pgxBackends) Drain(addr string) { b.pools.DrainAsync(addr) }
