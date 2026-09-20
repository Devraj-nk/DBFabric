//go:build chaos

package chaos

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"dbfabric/internal/app"
	"dbfabric/internal/config"
	"dbfabric/internal/shardmap"
)

// The health checker's timing in these tests: sweep every 500ms, DOWN after
// three consecutive misses, so a dead node is declared down in roughly
// 1.5-2s. Scenarios wait on state, not fixed sleeps, but their timeouts are
// sized around this.
const healthInterval = 500 * time.Millisecond

type proxyHandle struct {
	app  *app.App
	addr string
}

// startProxy runs the real proxy in-process in front of one shard whose
// primary is primaryAddr (which may be a forwarder rather than the real
// primary) and whose only replica is replicaAddr.
func startProxy(t *testing.T, primaryAddr, replicaAddr string, tweak func(*config.Config)) *proxyHandle {
	t.Helper()
	cfg := &config.Config{
		ListenAddr:          "127.0.0.1:" + strconv.Itoa(freePort(t)),
		HealthCheckInterval: healthInterval,
		Health: config.HealthConfig{
			SuspectAfterMisses: 1,
			DownAfterMisses:    3,
			PgPromote:          true,
			QuorumGuard:        true,
		},
		Routing: config.RoutingConfig{DefaultMaxLagMS: 1000},
		Backend: config.BackendConfig{User: "postgres", Database: "postgres", SSLMode: "disable"},
		Shards:  []config.ShardConfig{{ID: "shard-0", Primary: primaryAddr, Replicas: []string{replicaAddr}}},
	}
	if tweak != nil {
		tweak(cfg)
	}

	a := app.New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Log("proxy did not shut down within 15s")
		}
	})

	deadline := time.Now().Add(10 * time.Second)
	for {
		c, err := net.DialTimeout("tcp", cfg.ListenAddr, 200*time.Millisecond)
		if err == nil {
			c.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("proxy never started listening on %s", cfg.ListenAddr)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return &proxyHandle{app: a, addr: cfg.ListenAddr}
}

func (p *proxyHandle) shard() *shardmap.Shard {
	s, _ := p.app.ShardMap.Get("shard-0")
	return s
}

func waitUntil(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

// waitAllUp waits for the first health sweeps to mark the primary and the
// replica UP, so scenarios start from a known-healthy cluster.
func (p *proxyHandle) waitAllUp(t *testing.T) {
	t.Helper()
	waitUntil(t, 20*time.Second, "primary and replica both UP", func() bool {
		s := p.shard()
		return s.Primary.Status == shardmap.StatusUp &&
			len(s.Replicas) == 1 && s.Replicas[0].Status == shardmap.StatusUp
	})
}

// ---------------------------------------------------------------------------

// Kill the primary while four writers are mid-burst. Measures how long
// writes are unavailable and what the client-visible consequences are.
func TestChaos_KillPrimaryMidWriteBurst(t *testing.T) {
	tools := findPGTools(t)
	c := newCluster(t, tools)
	px := startProxy(t, c.primary.addr(), c.replica.addr(), nil)
	px.waitAllUp(t)

	w := startWorkload(t, px.addr, 4)
	time.Sleep(2 * time.Second)
	if w.ackCount() == 0 {
		t.Fatal("no writes were acknowledged before the kill: the harness is broken, not the proxy")
	}

	killStart := time.Now()
	c.primary.kill()
	killed := time.Now()

	waitUntil(t, 30*time.Second, "the shard map to fail over to the replica", func() bool {
		return px.shard().Primary.Addr == c.replica.addr()
	})
	failedOver := time.Now()

	waitUntil(t, 20*time.Second, "writes to resume", func() bool {
		_, ok := w.firstAckAfter(killed)
		return ok
	})
	time.Sleep(3 * time.Second)
	w.stop()

	// --- invariants ---
	if c.replica.inRecovery() {
		t.Error("the promoted node is still in recovery: routing moved to a node that cannot take writes")
	}
	if got := px.shard().Primary.Addr; got != c.replica.addr() {
		t.Errorf("shard primary = %s, want the promoted replica %s", got, c.replica.addr())
	}

	// --- measurements ---
	final := c.replica.rowCounts()
	before, after := w.ackedSplit(failedOver)
	lostBefore, lostAfter := 0, 0
	for _, id := range before {
		if final[id] == 0 {
			lostBefore++
		}
	}
	for _, id := range after {
		if final[id] == 0 {
			lostAfter++
		}
	}
	duplicated := 0
	for _, n := range final {
		if n > 1 {
			duplicated++
		}
	}

	stall, stallBegan := w.longestGap()
	t.Logf("write stall: %s with no acknowledged write (began %s after the kill command was issued); routing switched %s after it",
		stall.Round(time.Millisecond), stallBegan.Sub(killStart).Round(time.Millisecond), failedOver.Sub(killStart).Round(time.Millisecond))
	t.Logf("writes: %d acknowledged, %d attempts, %d client-visible errors", w.ackCount(), w.attemptCount(), w.errorCount())
	t.Logf("acknowledged writes missing on the new primary: %d of %d acked before failover (loss to asynchronous replication is possible in principle), %d of %d acked after (would be a proxy bug)",
		lostBefore, len(before), lostAfter, len(after))
	t.Logf("request ids present more than once (a retry of a write that had committed): %d", duplicated)

	if lostAfter != 0 {
		t.Errorf("%d write(s) acknowledged after failover are missing from the new primary: the proxy acked writes it did not durably apply", lostAfter)
	}
	if stall > 20*time.Second {
		t.Errorf("writes stalled for %s, want under 20s", stall)
	}
}

// Kill a replica. Writes go to the primary and must not notice; reads
// hinted eventual must stop being routed to the dead replica.
func TestChaos_KillReplica(t *testing.T) {
	tools := findPGTools(t)
	c := newCluster(t, tools)
	px := startProxy(t, c.primary.addr(), c.replica.addr(), nil)
	px.waitAllUp(t)

	w := startWorkload(t, px.addr, 4)
	r := startReadLoop(t, px.addr)
	time.Sleep(2 * time.Second)
	if okReads, errs := r.totals(); okReads == 0 || errs != 0 {
		t.Fatalf("before the kill: %d ok reads, %d errors (first: %v); want reads working cleanly", okReads, errs, r.firstError())
	}

	c.replica.kill()
	killed := time.Now()

	waitUntil(t, 20*time.Second, "the replica to be marked DOWN", func() bool {
		s := px.shard()
		return len(s.Replicas) == 1 && s.Replicas[0].Status == shardmap.StatusDown
	})
	markedDown := time.Now()
	time.Sleep(3 * time.Second)
	w.stop()
	r.stop()

	// Reads chosen for the dead replica before it was marked DOWN fail;
	// the proxy does not retry them elsewhere. After the mark, none should.
	settle := markedDown.Add(750 * time.Millisecond)
	lateReadErrs := r.errorsAfter(settle)
	totalOK, totalErrs := r.totals()

	t.Logf("replica declared DOWN %s after the kill", markedDown.Sub(killed).Round(time.Millisecond))
	t.Logf("reads: %d ok, %d failed (all of them in the detection window, before the replica was marked DOWN)", totalOK, totalErrs)
	t.Logf("writes: %d acknowledged, %d client-visible errors", w.ackCount(), w.errorCount())

	if w.errorCount() != 0 {
		t.Errorf("%d write error(s): losing a replica must not disturb writes to the primary", w.errorCount())
	}
	if lateReadErrs != 0 {
		t.Errorf("%d read(s) failed after the replica was marked DOWN: routing should have moved them to the primary", lateReadErrs)
	}
	if got := px.shard().Primary.Addr; got != c.primary.addr() {
		t.Errorf("primary changed to %s: a replica failure must not trigger failover", got)
	}
	if c.primary.inRecovery() {
		t.Error("primary unexpectedly in recovery")
	}
}

// Partition the proxy from a primary that is still alive and still feeding
// its replica. With the split-brain guard on, the replica's WAL receiver
// keeps reporting "streaming", the guard vetoes promotion, and the shard
// simply stays unavailable for writes until the partition heals.
func TestChaos_PartitionFromLivePrimary_GuardPreventsSplitBrain(t *testing.T) {
	tools := findPGTools(t)
	c := newCluster(t, tools)
	fwd := newForwarder(t, c.primary.addr())
	px := startProxy(t, fwd.addr(), c.replica.addr(), nil) // guard on
	px.waitAllUp(t)

	w := startWorkload(t, px.addr, 4)
	time.Sleep(2 * time.Second)
	if w.ackCount() == 0 {
		t.Fatal("no writes acknowledged before the partition: the harness is broken")
	}

	fwd.Partition()
	partitioned := time.Now()
	waitUntil(t, 20*time.Second, "the proxy to mark the unreachable primary DOWN", func() bool {
		return px.shard().Primary.Status == shardmap.StatusDown
	})
	ackedAtDown := w.ackCount()

	// Give the failover loop several chances to (wrongly) promote.
	time.Sleep(4 * time.Second)

	if got := px.shard().Primary.Addr; got != fwd.addr() {
		t.Errorf("shard primary moved to %s: the guard should have vetoed promotion while the replica still sees the primary", got)
	}
	if !c.replica.inRecovery() {
		t.Error("the replica was promoted despite still streaming from a live primary")
	}
	if got := w.ackCount(); got != ackedAtDown {
		t.Errorf("%d write(s) were acknowledged while the primary was unreachable", got-ackedAtDown)
	}
	if got := c.primary.query1("SELECT pg_is_in_recovery()::text"); got != "false" {
		t.Errorf("the real primary reports in_recovery=%s, want false", got)
	}

	fwd.Heal()
	healed := time.Now()
	waitUntil(t, 20*time.Second, "the primary to be marked UP again", func() bool {
		return px.shard().Primary.Status == shardmap.StatusUp
	})
	ackedAtHeal := w.ackCount()
	waitUntil(t, 20*time.Second, "writes to resume after the heal", func() bool {
		return w.ackCount() > ackedAtHeal
	})
	time.Sleep(2 * time.Second)
	w.stop()

	final := c.primary.rowCounts()
	lost := 0
	before, after := w.ackedSplit(time.Time{})
	for _, id := range append(before, after...) {
		if final[id] == 0 {
			lost++
		}
	}
	duplicated := 0
	for _, n := range final {
		if n > 1 {
			duplicated++
		}
	}
	t.Logf("partition lasted %s and was survived without promotion; writes resumed after the heal", healed.Sub(partitioned).Round(time.Millisecond))
	t.Logf("writes: %d acknowledged, %d client-visible errors (the outage: the price of not splitting the brain)", w.ackCount(), w.errorCount())
	t.Logf("request ids present more than once (a retry of a write that had committed before its connection was cut): %d", duplicated)
	if lost != 0 {
		t.Errorf("%d acknowledged write(s) missing from the primary, which never crashed", lost)
	}
	waitUntil(t, 20*time.Second, "the replica to converge with the primary", func() bool {
		return len(c.replica.rowCounts()) == len(final)
	})
}

// The same partition with the guard switched off: the proxy promotes the
// replica while the old primary is still alive and reachable by other
// clients, so two nodes accept writes and their data diverges. This is a
// demonstration of what the guard prevents, asserted so it can't silently
// stop being true.
func TestChaos_PartitionFromLivePrimary_WithoutGuardSplitsBrain(t *testing.T) {
	tools := findPGTools(t)
	c := newCluster(t, tools)
	fwd := newForwarder(t, c.primary.addr())
	px := startProxy(t, fwd.addr(), c.replica.addr(), func(cfg *config.Config) {
		cfg.Health.QuorumGuard = false
	})
	px.waitAllUp(t)

	fwd.Partition()
	waitUntil(t, 30*time.Second, "the proxy to promote the replica (no guard)", func() bool {
		return px.shard().Primary.Addr == c.replica.addr()
	})

	if c.replica.inRecovery() {
		t.Fatal("the shard map moved to the replica but it is not writable")
	}
	if c.primary.inRecovery() {
		t.Fatal("the old primary is unexpectedly in recovery")
	}

	// A client that can still reach the old primary keeps writing to it;
	// the proxy's clients now write to the promoted replica.
	c.primary.exec("INSERT INTO writes(req_id) VALUES (-1)")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := dialProxy(ctx, px.addr, "user-1")
	if err != nil {
		t.Fatalf("dialing the proxy after failover: %v", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "INSERT INTO writes(req_id) VALUES (-2)"); err != nil {
		t.Fatalf("write through the proxy after failover: %v", err)
	}

	oldSide, newSide := c.primary.rowCounts(), c.replica.rowCounts()
	t.Logf("split brain reproduced: old primary holds {-1:%d, -2:%d}, promoted node holds {-1:%d, -2:%d}",
		oldSide[-1], oldSide[-2], newSide[-1], newSide[-2])

	if oldSide[-1] != 1 || oldSide[-2] != 0 {
		t.Errorf("old primary rows = %v, want the direct write (-1) only", oldSide)
	}
	if newSide[-2] != 1 || newSide[-1] != 0 {
		t.Errorf("promoted node rows = %v, want the proxied write (-2) only", newSide)
	}
}
