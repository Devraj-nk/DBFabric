package router

import (
	"fmt"
	"testing"

	"dbfabric/internal/shardmap"
)

func newTestRouter() *Router {
	sm := shardmap.New()
	r := New(sm)
	r.AddShard(&shardmap.Shard{
		ID:      "shard-0",
		Primary: shardmap.Node{Addr: "primary-0"},
		Replicas: []shardmap.Node{
			{Addr: "replica-0a"},
			{Addr: "replica-0b"},
		},
	})
	r.AddShard(&shardmap.Shard{
		ID:      "shard-1",
		Primary: shardmap.Node{Addr: "primary-1"},
	})
	return r
}

// keyForShard brute-forces a shard key that hashes onto the given
// shard ID, so tests can target a specific shard's routing behavior.
func keyForShard(t *testing.T, r *Router, shardID string) string {
	t.Helper()
	for i := 0; i < 10000; i++ {
		k := fmt.Sprintf("key-%d", i)
		if got, _ := r.ring.ShardFor(k); got == shardID {
			return k
		}
	}
	t.Fatalf("couldn't find a key mapping to shard %q", shardID)
	return ""
}

func TestResolveStrongAlwaysPrimary(t *testing.T) {
	r := newTestRouter()
	node, err := r.Resolve("user-1", Strong, 0)
	if err != nil {
		t.Fatal(err)
	}
	if node.Addr != "primary-0" && node.Addr != "primary-1" {
		t.Fatalf("expected a primary, got %q", node.Addr)
	}
}

func TestResolveEventualPicksReplicaWhenAvailable(t *testing.T) {
	r := newTestRouter()
	key := keyForShard(t, r, "shard-0")

	node, err := r.Resolve(key, Eventual, 0)
	if err != nil {
		t.Fatal(err)
	}
	if node.Addr != "replica-0a" && node.Addr != "replica-0b" {
		t.Fatalf("expected a replica of shard-0, got %q", node.Addr)
	}
}

func TestResolveEventualFallsBackToPrimaryWithoutReplicas(t *testing.T) {
	r := newTestRouter()
	key := keyForShard(t, r, "shard-1")

	node, err := r.Resolve(key, Eventual, 0)
	if err != nil {
		t.Fatal(err)
	}
	if node.Addr != "primary-1" {
		t.Fatalf("expected fallback to primary-1, got %q", node.Addr)
	}
}

func TestResolveEventualSkipsDownReplicas(t *testing.T) {
	sm := shardmap.New()
	r := New(sm)
	r.AddShard(&shardmap.Shard{
		ID:      "shard-0",
		Primary: shardmap.Node{Addr: "primary-0"},
		Replicas: []shardmap.Node{
			{Addr: "replica-0a", Status: shardmap.StatusDown},
			{Addr: "replica-0b", Status: shardmap.StatusUp},
		},
	})

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key-%d", i)
		node, err := r.Resolve(key, Eventual, 0)
		if err != nil {
			t.Fatal(err)
		}
		if node.Addr == "replica-0a" {
			t.Fatalf("Resolve picked a DOWN replica for key %q", key)
		}
	}
}

func TestResolveEventualFallsBackToPrimaryWhenAllReplicasDown(t *testing.T) {
	sm := shardmap.New()
	r := New(sm)
	r.AddShard(&shardmap.Shard{
		ID:      "shard-0",
		Primary: shardmap.Node{Addr: "primary-0"},
		Replicas: []shardmap.Node{
			{Addr: "replica-0a", Status: shardmap.StatusDown},
			{Addr: "replica-0b", Status: shardmap.StatusDown},
		},
	})

	node, err := r.Resolve("user-1", Eventual, 0)
	if err != nil {
		t.Fatal(err)
	}
	if node.Addr != "primary-0" {
		t.Fatalf("expected fallback to primary-0 when all replicas are down, got %q", node.Addr)
	}
}

func TestResolveBoundedStalenessPrefersLowLagReplica(t *testing.T) {
	sm := shardmap.New()
	r := New(sm)
	r.AddShard(&shardmap.Shard{
		ID:      "shard-0",
		Primary: shardmap.Node{Addr: "primary-0"},
		Replicas: []shardmap.Node{
			{Addr: "replica-0a", LagMS: 5000}, // over threshold
			{Addr: "replica-0b", LagMS: 100},  // under threshold
		},
	})

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key-%d", i)
		node, err := r.Resolve(key, BoundedStaleness, 1000)
		if err != nil {
			t.Fatal(err)
		}
		if node.Addr == "replica-0a" {
			t.Fatalf("Resolve picked a replica over the lag threshold for key %q", key)
		}
	}
}

func TestResolveBoundedStalenessFallsBackToPrimaryWhenNoReplicaQualifies(t *testing.T) {
	sm := shardmap.New()
	r := New(sm)
	r.AddShard(&shardmap.Shard{
		ID:      "shard-0",
		Primary: shardmap.Node{Addr: "primary-0"},
		Replicas: []shardmap.Node{
			{Addr: "replica-0a", LagMS: 5000},
		},
	})

	node, err := r.Resolve("user-1", BoundedStaleness, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if node.Addr != "primary-0" {
		t.Fatalf("expected fallback to primary-0, got %q", node.Addr)
	}
}

func TestResolveNoShardsRegistered(t *testing.T) {
	sm := shardmap.New()
	r := New(sm)
	if _, err := r.Resolve("user-1", Strong, 0); err == nil {
		t.Fatal("expected an error when no shards are registered")
	}
}

func TestResolveUnknownConsistency(t *testing.T) {
	r := newTestRouter()
	if _, err := r.Resolve("user-1", Consistency("nonsense"), 0); err == nil {
		t.Fatal("expected an error for an unknown consistency level")
	}
}

func TestResolveSameKeyStableRoute(t *testing.T) {
	r := newTestRouter()
	first, err := r.Resolve("user-99", Strong, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		got, err := r.Resolve("user-99", Strong, 0)
		if err != nil {
			t.Fatal(err)
		}
		if got.Addr != first.Addr {
			t.Fatalf("routing for the same key changed: %q then %q", first.Addr, got.Addr)
		}
	}
}
