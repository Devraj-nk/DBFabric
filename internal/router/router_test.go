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
	node, err := r.Resolve("user-1", Strong)
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

	node, err := r.Resolve(key, Eventual)
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

	node, err := r.Resolve(key, Eventual)
	if err != nil {
		t.Fatal(err)
	}
	if node.Addr != "primary-1" {
		t.Fatalf("expected fallback to primary-1, got %q", node.Addr)
	}
}

func TestResolveBoundedStalenessFallsBackToPrimary(t *testing.T) {
	r := newTestRouter()
	node, err := r.Resolve("user-1", BoundedStaleness)
	if err != nil {
		t.Fatal(err)
	}
	if node.Addr != "primary-0" && node.Addr != "primary-1" {
		t.Fatalf("expected a primary (no lag tracking wired up yet), got %q", node.Addr)
	}
}

func TestResolveNoShardsRegistered(t *testing.T) {
	sm := shardmap.New()
	r := New(sm)
	if _, err := r.Resolve("user-1", Strong); err == nil {
		t.Fatal("expected an error when no shards are registered")
	}
}

func TestResolveUnknownConsistency(t *testing.T) {
	r := newTestRouter()
	if _, err := r.Resolve("user-1", Consistency("nonsense")); err == nil {
		t.Fatal("expected an error for an unknown consistency level")
	}
}

func TestResolveSameKeyStableRoute(t *testing.T) {
	r := newTestRouter()
	first, err := r.Resolve("user-99", Strong)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		got, err := r.Resolve("user-99", Strong)
		if err != nil {
			t.Fatal(err)
		}
		if got.Addr != first.Addr {
			t.Fatalf("routing for the same key changed: %q then %q", first.Addr, got.Addr)
		}
	}
}
