package health

import (
	"context"
	"net"
	"testing"
	"time"

	"dbfabric/internal/shardmap"
)

// freeAddr returns a TCP address guaranteed to refuse connections: it
// briefly listens on an OS-assigned port, then closes it. Used so
// these tests can exercise real (fast, local) connection failures
// without needing an actual Postgres server — this package's tests
// stay hermetic the same way internal/proxy's do.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func TestCheckerMarksNodeSuspectAfterOneMiss(t *testing.T) {
	primaryAddr := freeAddr(t)
	sm := shardmap.New()
	sm.Set("shard-0", &shardmap.Shard{ID: "shard-0", Primary: shardmap.Node{Addr: primaryAddr}})

	c := NewChecker(sm, newTestManager(), time.Second)
	c.sweep(context.Background())

	shard, _ := sm.Get("shard-0")
	if shard.Primary.Status != shardmap.StatusSuspect {
		t.Errorf("status after 1 miss = %q, want %q", shard.Primary.Status, shardmap.StatusSuspect)
	}
}

func TestCheckerMarksNodeDownAfterThreeMisses(t *testing.T) {
	primaryAddr := freeAddr(t)
	sm := shardmap.New()
	sm.Set("shard-0", &shardmap.Shard{ID: "shard-0", Primary: shardmap.Node{Addr: primaryAddr}})

	c := NewChecker(sm, newTestManager(), time.Second)
	for i := 0; i < downAfterMisses; i++ {
		c.sweep(context.Background())
	}

	shard, _ := sm.Get("shard-0")
	if shard.Primary.Status != shardmap.StatusDown {
		t.Errorf("status after %d misses = %q, want %q", downAfterMisses, shard.Primary.Status, shardmap.StatusDown)
	}
}

func TestCheckerAttemptsFailoverWhenPrimaryDies(t *testing.T) {
	primaryAddr := freeAddr(t)
	replicaAddr := freeAddr(t) // also unreachable, so promotion will fail — see below
	sm := shardmap.New()
	sm.Set("shard-0", &shardmap.Shard{
		ID:       "shard-0",
		Primary:  shardmap.Node{Addr: primaryAddr},
		Replicas: []shardmap.Node{{Addr: replicaAddr}},
	})

	c := NewChecker(sm, newTestManager(), time.Second)
	for i := 0; i < downAfterMisses; i++ {
		c.sweep(context.Background())
	}

	shard, _ := sm.Get("shard-0")
	if shard.Primary.Status != shardmap.StatusDown {
		t.Fatalf("primary status = %q, want %q", shard.Primary.Status, shardmap.StatusDown)
	}
	// The replica is equally unreachable, so it hits DOWN in the same
	// sweep as the primary and Promote has nothing healthy to pick —
	// this exercises the checker's failover *trigger* wiring reaching
	// Controller.Promote and handling its error without panicking, not
	// a successful promotion (that needs a reachable replica, which
	// needs a real Postgres — see work.md for how that was verified
	// manually instead of here).
	if shard.Primary.Addr != primaryAddr {
		t.Errorf("primary addr changed to %q despite no healthy replica to promote", shard.Primary.Addr)
	}
}

func TestCheckerDoesNotReattemptFailoverOnceAlreadyDown(t *testing.T) {
	primaryAddr := freeAddr(t)
	sm := shardmap.New()
	sm.Set("shard-0", &shardmap.Shard{ID: "shard-0", Primary: shardmap.Node{Addr: primaryAddr}})

	c := NewChecker(sm, newTestManager(), time.Second)
	for i := 0; i < downAfterMisses; i++ {
		c.sweep(context.Background())
	}
	shard, _ := sm.Get("shard-0")
	if shard.Primary.Status != shardmap.StatusDown {
		t.Fatalf("precondition failed: primary should be DOWN, got %q", shard.Primary.Status)
	}

	// Further sweeps must leave the (already-DOWN, replica-less) shard
	// stable rather than erroring/looping oddly on a repeated trigger.
	for i := 0; i < 3; i++ {
		c.sweep(context.Background())
	}
	shard, _ = sm.Get("shard-0")
	if shard.Primary.Status != shardmap.StatusDown || shard.Primary.Addr != primaryAddr {
		t.Errorf("shard state drifted after repeated sweeps: %+v", shard.Primary)
	}
}
