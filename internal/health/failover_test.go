package health

import (
	"context"
	"errors"
	"testing"

	"dbfabric/internal/shardmap"
)

func TestSelectPromotionCandidatePicksLowestLag(t *testing.T) {
	replicas := []shardmap.Node{
		{Addr: "r-a", LagMS: 500},
		{Addr: "r-b", LagMS: 50},
		{Addr: "r-c", LagMS: 900},
	}
	got, remaining, ok := selectPromotionCandidate(replicas)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if got.Addr != "r-b" {
		t.Errorf("candidate = %q, want %q (lowest lag)", got.Addr, "r-b")
	}
	if len(remaining) != 2 {
		t.Fatalf("len(remaining) = %d, want 2", len(remaining))
	}
	for _, r := range remaining {
		if r.Addr == "r-b" {
			t.Error("remaining still contains the promoted replica")
		}
	}
}

func TestSelectPromotionCandidateSkipsDownReplicas(t *testing.T) {
	replicas := []shardmap.Node{
		{Addr: "r-a", LagMS: 1, Status: shardmap.StatusDown}, // lowest lag but DOWN
		{Addr: "r-b", LagMS: 900, Status: shardmap.StatusUp},
	}
	got, _, ok := selectPromotionCandidate(replicas)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if got.Addr != "r-b" {
		t.Errorf("candidate = %q, want %q (only non-DOWN option)", got.Addr, "r-b")
	}
}

func TestSelectPromotionCandidateNoneQualify(t *testing.T) {
	tests := [][]shardmap.Node{
		nil,
		{{Addr: "r-a", Status: shardmap.StatusDown}},
	}
	for _, replicas := range tests {
		if _, _, ok := selectPromotionCandidate(replicas); ok {
			t.Errorf("selectPromotionCandidate(%v) = ok, want ok=false", replicas)
		}
	}
}

func TestQuorumAllowsPromotion(t *testing.T) {
	tests := []struct {
		name                  string
		sawPrimary, cannotSee int
		want                  bool
	}{
		{"no witness answered vetoes", 0, 0, false},
		{"single witness cannot see primary", 0, 1, true},
		{"single witness still sees primary", 1, 0, false},
		{"majority cannot see", 1, 2, true},
		{"majority still sees", 2, 1, false},
		{"tie vetoes", 1, 1, false},
		{"unanimous cannot see", 0, 3, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := quorumAllowsPromotion(tt.sawPrimary, tt.cannotSee); got != tt.want {
				t.Errorf("quorumAllowsPromotion(%d, %d) = %v, want %v", tt.sawPrimary, tt.cannotSee, got, tt.want)
			}
		})
	}
}

// threeNodeShard registers shard-0 with one primary and the given replicas.
func threeNodeShard(sm *shardmap.Map, replicas ...shardmap.Node) {
	sm.Set("shard-0", &shardmap.Shard{
		ID:       "shard-0",
		Primary:  shardmap.Node{Addr: "old-primary", Status: shardmap.StatusDown},
		Replicas: replicas,
	})
}

func TestControllerPromoteSuccessOrdersGuardPromoteDrain(t *testing.T) {
	sm := shardmap.New()
	threeNodeShard(sm,
		shardmap.Node{Addr: "r-a", LagMS: 500, Status: shardmap.StatusUp},
		shardmap.Node{Addr: "r-b", LagMS: 50, Status: shardmap.StatusUp},
	)
	ops := newFakeBackends() // neither replica streaming: both cannot see the primary
	c := newController(sm, ops, true, true)

	if err := c.Promote(context.Background(), "shard-0"); err != nil {
		t.Fatal(err)
	}

	shard, _ := sm.Get("shard-0")
	if shard.Primary.Addr != "r-b" || shard.Primary.Status != shardmap.StatusUp {
		t.Errorf("primary = %+v, want r-b UP (lowest lag)", shard.Primary)
	}
	if len(shard.Replicas) != 1 || shard.Replicas[0].Addr != "r-a" {
		t.Errorf("replicas = %v, want just r-a", shard.Replicas)
	}

	calls := ops.callLog()
	promoteAt, drainAt := indexOf(calls, "promote:r-b"), indexOf(calls, "drain:old-primary")
	if promoteAt < 0 || drainAt < 0 {
		t.Fatalf("expected promote:r-b and drain:old-primary in %v", calls)
	}
	if indexOf(calls, "witness:r-a") < 0 || indexOf(calls, "witness:r-b") < 0 {
		t.Errorf("expected both replicas to be polled as witnesses, got %v", calls)
	}
	for _, w := range []string{"witness:r-a", "witness:r-b"} {
		if indexOf(calls, w) > promoteAt {
			t.Errorf("%s happened after promote — the guard must vote first: %v", w, calls)
		}
	}
	if promoteAt > drainAt {
		t.Errorf("drain happened before promote: %v", calls)
	}
}

func TestControllerPromoteUnknownShard(t *testing.T) {
	c := newController(shardmap.New(), newFakeBackends(), true, true)
	if err := c.Promote(context.Background(), "no-such-shard"); err == nil {
		t.Fatal("expected an error for an unknown shard")
	}
}

func TestControllerPromoteNoHealthyReplica(t *testing.T) {
	sm := shardmap.New()
	threeNodeShard(sm, shardmap.Node{Addr: "r-a", Status: shardmap.StatusDown})
	ops := newFakeBackends()
	c := newController(sm, ops, true, true)

	if err := c.Promote(context.Background(), "shard-0"); err == nil {
		t.Fatal("expected an error when no replica is healthy")
	}
	shard, _ := sm.Get("shard-0")
	if shard.Primary.Addr != "old-primary" {
		t.Errorf("shard mutated despite a failed promotion: primary = %q", shard.Primary.Addr)
	}
	if got := ops.callLog(); len(got) != 0 {
		t.Errorf("no backend should be touched when there is no candidate, got %v", got)
	}
}

func TestControllerGuardVetoesWhenMajorityStillSeesPrimary(t *testing.T) {
	sm := shardmap.New()
	threeNodeShard(sm,
		shardmap.Node{Addr: "r-a", Status: shardmap.StatusUp},
		shardmap.Node{Addr: "r-b", Status: shardmap.StatusUp},
		shardmap.Node{Addr: "r-c", Status: shardmap.StatusUp},
	)
	ops := newFakeBackends()
	ops.setStreaming("r-a", true)
	ops.setStreaming("r-b", true)
	c := newController(sm, ops, true, true)

	err := c.Promote(context.Background(), "shard-0")
	if !errors.Is(err, ErrPromotionVetoed) {
		t.Fatalf("err = %v, want ErrPromotionVetoed", err)
	}
	shard, _ := sm.Get("shard-0")
	if shard.Primary.Addr != "old-primary" {
		t.Errorf("shard mutated despite a veto: primary = %q", shard.Primary.Addr)
	}
	if ops.count("promote:r-a")+ops.count("promote:r-b")+ops.count("promote:r-c") != 0 {
		t.Error("pg_promote was called despite the veto")
	}
	if ops.count("drain:old-primary") != 0 {
		t.Error("old primary was drained despite the veto")
	}
}

func TestControllerGuardAllowsWhenMajorityCannotSeePrimary(t *testing.T) {
	sm := shardmap.New()
	threeNodeShard(sm,
		shardmap.Node{Addr: "r-a", Status: shardmap.StatusUp},
		shardmap.Node{Addr: "r-b", Status: shardmap.StatusUp},
		shardmap.Node{Addr: "r-c", Status: shardmap.StatusUp},
	)
	ops := newFakeBackends()
	ops.setStreaming("r-c", true) // one dissenter out of three
	c := newController(sm, ops, true, true)

	if err := c.Promote(context.Background(), "shard-0"); err != nil {
		t.Fatalf("2-of-3 replicas cannot see the primary; promotion should proceed: %v", err)
	}
}

func TestControllerGuardAbstainingWitnessesDoNotCount(t *testing.T) {
	sm := shardmap.New()
	threeNodeShard(sm,
		shardmap.Node{Addr: "r-a", Status: shardmap.StatusUp},
		shardmap.Node{Addr: "r-b", Status: shardmap.StatusUp},
	)
	ops := newFakeBackends()
	ops.streamErr["r-b"] = errUnreachable // abstains; r-a alone cannot see the primary
	c := newController(sm, ops, true, true)

	if err := c.Promote(context.Background(), "shard-0"); err != nil {
		t.Fatalf("one answering witness that cannot see the primary should suffice: %v", err)
	}

	sm2 := shardmap.New()
	threeNodeShard(sm2, shardmap.Node{Addr: "r-a", Status: shardmap.StatusUp})
	ops2 := newFakeBackends()
	ops2.streamErr["r-a"] = errUnreachable // nobody answers
	c2 := newController(sm2, ops2, true, true)
	if err := c2.Promote(context.Background(), "shard-0"); !errors.Is(err, ErrPromotionVetoed) {
		t.Fatalf("with no answering witness the guard must veto, got %v", err)
	}
}

func TestControllerGuardDoesNotPollDownReplicas(t *testing.T) {
	sm := shardmap.New()
	threeNodeShard(sm,
		shardmap.Node{Addr: "r-a", Status: shardmap.StatusUp},
		shardmap.Node{Addr: "r-dead", Status: shardmap.StatusDown},
	)
	ops := newFakeBackends()
	c := newController(sm, ops, true, true)

	if err := c.Promote(context.Background(), "shard-0"); err != nil {
		t.Fatal(err)
	}
	if ops.count("witness:r-dead") != 0 {
		t.Error("a DOWN replica was polled as a witness")
	}
}

func TestControllerPgPromoteFailureLeavesShardUntouched(t *testing.T) {
	sm := shardmap.New()
	threeNodeShard(sm, shardmap.Node{Addr: "r-a", Status: shardmap.StatusUp})
	ops := newFakeBackends()
	ops.setPromoteErr("r-a", errors.New("boom"))
	c := newController(sm, ops, true, true)

	if err := c.Promote(context.Background(), "shard-0"); err == nil {
		t.Fatal("expected pg_promote's failure to surface")
	}
	shard, _ := sm.Get("shard-0")
	if shard.Primary.Addr != "old-primary" || len(shard.Replicas) != 1 {
		t.Errorf("shard mutated after a failed pg_promote: %+v", shard)
	}
	if ops.count("drain:old-primary") != 0 {
		t.Error("old primary drained even though promotion failed")
	}
}

func TestControllerWithoutPgPromoteStillReroutes(t *testing.T) {
	sm := shardmap.New()
	threeNodeShard(sm, shardmap.Node{Addr: "r-a", Status: shardmap.StatusUp})
	ops := newFakeBackends()
	c := newController(sm, ops, false, true)

	if err := c.Promote(context.Background(), "shard-0"); err != nil {
		t.Fatal(err)
	}
	if ops.count("promote:r-a") != 0 {
		t.Error("pg_promote was issued although it is disabled")
	}
	shard, _ := sm.Get("shard-0")
	if shard.Primary.Addr != "r-a" {
		t.Errorf("primary = %q, want r-a", shard.Primary.Addr)
	}
}

func TestControllerWithoutGuardIgnoresStreamingReplicas(t *testing.T) {
	sm := shardmap.New()
	threeNodeShard(sm, shardmap.Node{Addr: "r-a", Status: shardmap.StatusUp})
	ops := newFakeBackends()
	ops.setStreaming("r-a", true)
	c := newController(sm, ops, true, false)

	if err := c.Promote(context.Background(), "shard-0"); err != nil {
		t.Fatalf("guard is off, promotion should proceed: %v", err)
	}
	if ops.count("witness:r-a") != 0 {
		t.Error("witnesses were polled although the guard is disabled")
	}
}

func indexOf(s []string, want string) int {
	for i, v := range s {
		if v == want {
			return i
		}
	}
	return -1
}
