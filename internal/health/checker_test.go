package health

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dbfabric/internal/pool"
	"dbfabric/internal/shardmap"
)

// addShard registers a shard whose nodes start with no status/lag, as they
// do when freshly loaded from config.
func addShard(sm *shardmap.Map, id, primary string, replicas ...string) {
	s := &shardmap.Shard{ID: id, Primary: shardmap.Node{Addr: primary}}
	for _, r := range replicas {
		s.Replicas = append(s.Replicas, shardmap.Node{Addr: r})
	}
	sm.Set(id, s)
}

func mustGet(t *testing.T, sm *shardmap.Map, id string) *shardmap.Shard {
	t.Helper()
	s, ok := sm.Get(id)
	if !ok {
		t.Fatalf("shard %q missing from the shard map", id)
	}
	return s
}

func sweepN(c *Checker, n int) {
	for i := 0; i < n; i++ {
		c.sweep(context.Background())
	}
}

func testOpts() Options {
	return Options{Interval: time.Second, PgPromote: true, QuorumGuard: true}
}

func TestCheckerHealthyNodesStayUpAndReportLag(t *testing.T) {
	sm := shardmap.New()
	addShard(sm, "shard-0", "p", "r")
	ops := newFakeBackends()
	ops.setLag("r", 120)

	c := newCheckerWithBackends(sm, ops, testOpts())
	sweepN(c, 1)

	s := mustGet(t, sm, "shard-0")
	if s.Primary.Status != shardmap.StatusUp {
		t.Errorf("primary status = %q, want UP", s.Primary.Status)
	}
	if s.Replicas[0].Status != shardmap.StatusUp || s.Replicas[0].LagMS != 120 {
		t.Errorf("replica = %+v, want UP with LagMS 120", s.Replicas[0])
	}
}

func TestCheckerMissThresholdsAreConfigurable(t *testing.T) {
	sm := shardmap.New()
	addShard(sm, "shard-0", "p") // no replicas: failover can't happen, we only watch status
	ops := newFakeBackends()
	ops.setDown("p", true)

	opts := testOpts()
	opts.SuspectAfterMisses = 2
	opts.DownAfterMisses = 4
	c := newCheckerWithBackends(sm, ops, opts)

	want := []shardmap.NodeStatus{shardmap.StatusUp, shardmap.StatusSuspect, shardmap.StatusSuspect, shardmap.StatusDown}
	for i, w := range want {
		sweepN(c, 1)
		if got := mustGet(t, sm, "shard-0").Primary.Status; got != w {
			t.Errorf("after %d miss(es): status = %q, want %q", i+1, got, w)
		}
	}
}

func TestCheckerSuccessResetsMissCount(t *testing.T) {
	sm := shardmap.New()
	addShard(sm, "shard-0", "p")
	ops := newFakeBackends()
	c := newCheckerWithBackends(sm, ops, testOpts()) // suspect@1, down@3

	ops.setDown("p", true)
	sweepN(c, 2)
	if got := mustGet(t, sm, "shard-0").Primary.Status; got != shardmap.StatusSuspect {
		t.Fatalf("after 2 misses: status = %q, want SUSPECT", got)
	}

	ops.setDown("p", false)
	sweepN(c, 1)
	if got := mustGet(t, sm, "shard-0").Primary.Status; got != shardmap.StatusUp {
		t.Fatalf("after a success: status = %q, want UP", got)
	}

	// Two more misses would be 4 in a row without the reset; with it
	// the count restarted, so the node is only SUSPECT, not DOWN.
	ops.setDown("p", true)
	sweepN(c, 2)
	if got := mustGet(t, sm, "shard-0").Primary.Status; got != shardmap.StatusSuspect {
		t.Errorf("after reset + 2 misses: status = %q, want SUSPECT (miss count should have restarted)", got)
	}
}

func TestCheckerKeepsLastKnownLagWhenAReplicaMisses(t *testing.T) {
	sm := shardmap.New()
	addShard(sm, "shard-0", "p", "r")
	ops := newFakeBackends()
	ops.setLag("r", 300)
	c := newCheckerWithBackends(sm, ops, testOpts())

	sweepN(c, 1)
	ops.setDown("r", true)
	sweepN(c, 1)

	r := mustGet(t, sm, "shard-0").Replicas[0]
	if r.Status != shardmap.StatusSuspect {
		t.Errorf("status = %q, want SUSPECT", r.Status)
	}
	if r.LagMS != 300 {
		t.Errorf("LagMS = %d, want the last known 300 (resetting to 0 would make a dead replica look fresh)", r.LagMS)
	}
}

func TestCheckerFailsOverWhenPrimaryDies(t *testing.T) {
	sm := shardmap.New()
	addShard(sm, "shard-0", "p", "r")
	ops := newFakeBackends()
	ops.setDown("p", true)
	ops.setLag("r", 10)
	c := newCheckerWithBackends(sm, ops, testOpts())

	sweepN(c, 2)
	if got := mustGet(t, sm, "shard-0").Primary.Addr; got != "p" {
		t.Fatalf("failed over after only 2 misses (primary = %q); threshold is 3", got)
	}
	sweepN(c, 1)

	s := mustGet(t, sm, "shard-0")
	if s.Primary.Addr != "r" || s.Primary.Status != shardmap.StatusUp {
		t.Fatalf("primary = %+v, want r UP", s.Primary)
	}
	if len(s.Replicas) != 0 {
		t.Errorf("replicas = %v, want none left", s.Replicas)
	}

	calls := ops.callLog()
	w, p, d := indexOf(calls, "witness:r"), indexOf(calls, "promote:r"), indexOf(calls, "drain:p")
	if w < 0 || p < 0 || d < 0 || !(w < p && p < d) {
		t.Errorf("want witness -> promote -> drain in order, got %v", calls)
	}
}

func TestCheckerFailoverIsNotRepeatedOnceCompleted(t *testing.T) {
	sm := shardmap.New()
	addShard(sm, "shard-0", "p", "r")
	ops := newFakeBackends()
	ops.setDown("p", true)
	c := newCheckerWithBackends(sm, ops, testOpts())

	sweepN(c, 3) // fails over
	sweepN(c, 3) // new primary "r" is healthy; nothing more to do

	if n := ops.count("promote:r"); n != 1 {
		t.Errorf("pg_promote issued %d times, want exactly once", n)
	}
}

func TestCheckerRetriesFailoverAfterNoHealthyReplica(t *testing.T) {
	sm := shardmap.New()
	addShard(sm, "shard-0", "p", "r")
	ops := newFakeBackends()
	ops.setDown("p", true)
	ops.setDown("r", true)
	c := newCheckerWithBackends(sm, ops, testOpts())

	sweepN(c, 3)
	s := mustGet(t, sm, "shard-0")
	if s.Primary.Addr != "p" || s.Primary.Status != shardmap.StatusDown {
		t.Fatalf("primary = %+v, want p DOWN (nothing to fail over to)", s.Primary)
	}

	// The replica comes back. A failover that only fired on the
	// moment the primary first went DOWN would never notice.
	ops.setDown("r", false)
	sweepN(c, 1)
	if got := mustGet(t, sm, "shard-0").Primary.Addr; got != "r" {
		t.Errorf("primary = %q, want r: failover must be retried while the primary stays DOWN", got)
	}
}

func TestCheckerGuardVetoIsRetriedAndEventuallyAllowed(t *testing.T) {
	sm := shardmap.New()
	addShard(sm, "shard-0", "p", "r")
	ops := newFakeBackends()
	ops.setDown("p", true)      // the proxy can't reach the primary...
	ops.setStreaming("r", true) // ...but the replica still can
	c := newCheckerWithBackends(sm, ops, testOpts())

	sweepN(c, 5)
	s := mustGet(t, sm, "shard-0")
	if s.Primary.Addr != "p" || s.Primary.Status != shardmap.StatusDown {
		t.Fatalf("primary = %+v, want p DOWN and untouched while the replica still sees it", s.Primary)
	}
	if n := ops.count("promote:r"); n != 0 {
		t.Fatalf("pg_promote issued %d times despite the guard's veto", n)
	}

	ops.setStreaming("r", false) // replica's WAL receiver finally drops
	sweepN(c, 1)
	if got := mustGet(t, sm, "shard-0").Primary.Addr; got != "r" {
		t.Errorf("primary = %q, want r once the replica also lost the primary", got)
	}
}

func TestCheckerHealedPartitionCancelsPendingFailover(t *testing.T) {
	sm := shardmap.New()
	addShard(sm, "shard-0", "p", "r")
	ops := newFakeBackends()
	ops.setDown("p", true)
	ops.setStreaming("r", true)
	c := newCheckerWithBackends(sm, ops, testOpts())

	sweepN(c, 4) // DOWN, vetoed every time
	ops.setDown("p", false)
	sweepN(c, 1) // the partition heals

	s := mustGet(t, sm, "shard-0")
	if s.Primary.Addr != "p" || s.Primary.Status != shardmap.StatusUp {
		t.Errorf("primary = %+v, want p back UP", s.Primary)
	}

	ops.setStreaming("r", false)
	sweepN(c, 3)
	if n := ops.count("promote:r"); n != 0 {
		t.Errorf("pg_promote issued %d times for a primary that is healthy again", n)
	}
}

func TestCheckerQuorumGuardDisabledPromotesRegardless(t *testing.T) {
	sm := shardmap.New()
	addShard(sm, "shard-0", "p", "r")
	ops := newFakeBackends()
	ops.setDown("p", true)
	ops.setStreaming("r", true)

	opts := testOpts()
	opts.QuorumGuard = false
	c := newCheckerWithBackends(sm, ops, opts)

	sweepN(c, 3)
	if got := mustGet(t, sm, "shard-0").Primary.Addr; got != "r" {
		t.Errorf("primary = %q, want r (guard is off)", got)
	}
}

func TestCheckerPgPromoteFailureIsRetried(t *testing.T) {
	sm := shardmap.New()
	addShard(sm, "shard-0", "p", "r")
	ops := newFakeBackends()
	ops.setDown("p", true)
	ops.setPromoteErr("r", errors.New("could not promote"))
	c := newCheckerWithBackends(sm, ops, testOpts())

	sweepN(c, 4)
	if got := mustGet(t, sm, "shard-0").Primary.Addr; got != "p" {
		t.Fatalf("primary = %q: routing must not move to a node whose pg_promote failed", got)
	}

	ops.setPromoteErr("r", nil)
	sweepN(c, 1)
	if got := mustGet(t, sm, "shard-0").Primary.Addr; got != "r" {
		t.Errorf("primary = %q, want r after pg_promote starts succeeding", got)
	}
}

func TestCheckerPingsNodesInParallel(t *testing.T) {
	sm := shardmap.New()
	addShard(sm, "shard-0", "p", "r1", "r2")
	ops := newFakeBackends()

	var inFlight atomic.Int32
	allIn := make(chan struct{})
	ops.pingHook = func(string) {
		if inFlight.Add(1) == 3 {
			close(allIn)
		}
		select {
		case <-allIn:
		case <-time.After(2 * time.Second):
		}
	}

	c := newCheckerWithBackends(sm, ops, testOpts())
	done := make(chan struct{})
	go func() {
		c.sweep(context.Background())
		close(done)
	}()

	select {
	case <-allIn:
	case <-time.After(time.Second):
		t.Fatal("the shard's three nodes were not pinged concurrently: the barrier never filled")
	}
	<-done
}

func TestCheckerSlowFailoverDoesNotBlockOtherShardsOrOverlap(t *testing.T) {
	sm := shardmap.New()
	addShard(sm, "shard-a", "a-p", "a-r")
	addShard(sm, "shard-b", "b-p", "b-r")
	ops := newFakeBackends()
	ops.setDown("a-p", true)
	ops.setLag("b-r", 111)

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	ops.promoteHook = func(string) {
		once.Do(func() { close(entered) })
		<-release
	}

	opts := testOpts()
	opts.QuorumGuard = false
	c := newCheckerWithBackends(sm, ops, opts)

	sweepN(c, 2) // a-p reaches 2 misses; the next sweep triggers failover

	// A value distinct from any earlier reading, so seeing it proves the
	// blocked sweep's own check of shard-b completed.
	ops.setLag("b-r", 333)
	firstDone := make(chan struct{})
	go func() {
		c.sweep(context.Background())
		close(firstDone)
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("failover for shard-a never started")
	}

	// shard-a is now stuck inside pg_promote. shard-b must still be
	// checked, and must finish (Set *and* release its busy flag) while
	// shard-a is still blocked.
	waitFor(t, "shard-b's check in the blocked sweep to finish", func() bool {
		return mustGet(t, sm, "shard-b").Replicas[0].LagMS == 333 && !c.isBusy("shard-b")
	})
	if !c.isBusy("shard-a") {
		t.Fatal("shard-a should still be busy: its failover is blocked")
	}

	ops.setLag("b-r", 222)
	secondDone := make(chan struct{})
	go func() {
		c.sweep(context.Background())
		close(secondDone)
	}()
	select {
	case <-secondDone:
	case <-time.After(2 * time.Second):
		t.Fatal("a second sweep blocked behind shard-a's in-flight failover")
	}
	if got := mustGet(t, sm, "shard-b").Replicas[0].LagMS; got != 222 {
		t.Errorf("shard-b lag = %d, want 222: it should be checked while shard-a is mid-failover", got)
	}
	if n := ops.count("promote:a-r"); n != 1 {
		t.Errorf("pg_promote issued %d times, want 1: shard-a must not be checked twice concurrently", n)
	}

	close(release)
	<-firstDone
	if got := mustGet(t, sm, "shard-a").Primary.Addr; got != "a-r" {
		t.Errorf("shard-a primary = %q, want a-r once the promotion is released", got)
	}
}

func TestCheckerRunSweepsImmediatelyAndStopsOnCancel(t *testing.T) {
	sm := shardmap.New()
	addShard(sm, "shard-0", "p", "r")
	ops := newFakeBackends()
	ops.setLag("r", 42)

	opts := testOpts()
	opts.Interval = time.Hour // so only the immediate sweep can populate anything
	c := newCheckerWithBackends(sm, ops, opts)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	waitFor(t, "the immediate sweep to record replica lag", func() bool {
		return mustGet(t, sm, "shard-0").Replicas[0].LagMS == 42
	})

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after its context was cancelled")
	}
}

// --- against real pgx pools, pointed at ports that refuse connections ---

// freeAddr returns a TCP address guaranteed to refuse connections: it
// briefly listens on an OS-assigned port, then closes it. This lets tests
// exercise the real pgx failure path — fast and local — without needing a
// running Postgres.
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

func newRealChecker(sm *shardmap.Map) *Checker {
	pm := pool.NewManager(pool.Backend{User: "app", Database: "appdb"})
	return NewChecker(sm, pm, Options{Interval: time.Second, QuorumGuard: true})
}

func TestRealBackendsRefusedConnectionCountsAsAMiss(t *testing.T) {
	sm := shardmap.New()
	addShard(sm, "shard-0", freeAddr(t))
	c := newRealChecker(sm)

	sweepN(c, 1)
	if got := mustGet(t, sm, "shard-0").Primary.Status; got != shardmap.StatusSuspect {
		t.Errorf("status after 1 refused ping = %q, want SUSPECT", got)
	}
}

func TestRealBackendsRefusedPrimaryGoesDownAfterThreeMisses(t *testing.T) {
	sm := shardmap.New()
	addShard(sm, "shard-0", freeAddr(t))
	c := newRealChecker(sm)

	sweepN(c, 3)
	if got := mustGet(t, sm, "shard-0").Primary.Status; got != shardmap.StatusDown {
		t.Errorf("status after 3 refused pings = %q, want DOWN", got)
	}
}

func (c *Checker) isBusy(shardID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.busy[shardID]
}

// waitFor polls cond until it is true or a deadline passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
