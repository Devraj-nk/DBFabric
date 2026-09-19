package health

import (
	"context"
	"testing"

	"dbfabric/internal/pool"
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

func newTestManager() *pool.Manager {
	return pool.NewManager(pool.Backend{User: "app", Database: "appdb"})
}

func TestControllerPromoteSuccess(t *testing.T) {
	sm := shardmap.New()
	sm.Set("shard-0", &shardmap.Shard{
		ID:      "shard-0",
		Primary: shardmap.Node{Addr: "old-primary"},
		Replicas: []shardmap.Node{
			{Addr: "r-a", LagMS: 500},
			{Addr: "r-b", LagMS: 50},
		},
	})
	pm := newTestManager()

	// Populate old-primary's pool so we can confirm Promote drains it.
	if _, err := pm.Get(context.Background(), "old-primary"); err != nil {
		t.Fatal(err)
	}
	before, _ := pm.Get(context.Background(), "old-primary")

	c := NewController(sm, pm)
	if err := c.Promote("shard-0"); err != nil {
		t.Fatal(err)
	}

	shard, ok := sm.Get("shard-0")
	if !ok {
		t.Fatal("shard-0 missing after promotion")
	}
	if shard.Primary.Addr != "r-b" {
		t.Errorf("new primary = %q, want %q (lowest lag)", shard.Primary.Addr, "r-b")
	}
	if shard.Primary.Status != shardmap.StatusUp {
		t.Errorf("new primary status = %q, want %q", shard.Primary.Status, shardmap.StatusUp)
	}
	if len(shard.Replicas) != 1 || shard.Replicas[0].Addr != "r-a" {
		t.Errorf("remaining replicas = %v, want just r-a", shard.Replicas)
	}
	for _, r := range shard.Replicas {
		if r.Addr == "old-primary" {
			t.Error("old primary reappeared as a replica")
		}
	}

	after, _ := pm.Get(context.Background(), "old-primary")
	if before == after {
		t.Error("old primary's pool was not drained by Promote")
	}
}

func TestControllerPromoteUnknownShard(t *testing.T) {
	c := NewController(shardmap.New(), newTestManager())
	if err := c.Promote("no-such-shard"); err == nil {
		t.Fatal("expected an error for an unknown shard")
	}
}

func TestControllerPromoteNoHealthyReplica(t *testing.T) {
	sm := shardmap.New()
	sm.Set("shard-0", &shardmap.Shard{
		ID:      "shard-0",
		Primary: shardmap.Node{Addr: "old-primary"},
		Replicas: []shardmap.Node{
			{Addr: "r-a", Status: shardmap.StatusDown},
		},
	})
	c := NewController(sm, newTestManager())

	if err := c.Promote("shard-0"); err == nil {
		t.Fatal("expected an error when no replica is healthy")
	}

	shard, _ := sm.Get("shard-0")
	if shard.Primary.Addr != "old-primary" {
		t.Errorf("shard was mutated despite a failed promotion: primary = %q", shard.Primary.Addr)
	}
}
