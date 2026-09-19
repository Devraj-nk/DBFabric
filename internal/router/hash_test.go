package router

import (
	"fmt"
	"testing"
)

func TestRingDistributesAcrossShards(t *testing.T) {
	ring := NewRing(100)
	ring.AddShard("shard-0")
	ring.AddShard("shard-1")
	ring.AddShard("shard-2")

	counts := map[string]int{}
	const n = 3000
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("user-%d", i)
		shard, ok := ring.ShardFor(key)
		if !ok {
			t.Fatalf("ShardFor(%q) returned ok=false", key)
		}
		counts[shard]++
	}

	if len(counts) != 3 {
		t.Fatalf("expected keys spread across 3 shards, got %d shards: %v", len(counts), counts)
	}
	for shard, c := range counts {
		if c < n/6 || c > n/2 {
			t.Errorf("shard %q got %d/%d keys, expected roughly even spread", shard, c, n)
		}
	}
}

func TestRingSameKeyStableAcrossCalls(t *testing.T) {
	ring := NewRing(100)
	ring.AddShard("shard-0")
	ring.AddShard("shard-1")

	first, _ := ring.ShardFor("user-42")
	for i := 0; i < 10; i++ {
		got, _ := ring.ShardFor("user-42")
		if got != first {
			t.Fatalf("ShardFor not stable: got %q then %q", first, got)
		}
	}
}

func TestRingMinimalReshuffleOnAddShard(t *testing.T) {
	ring := NewRing(100)
	ring.AddShard("shard-0")
	ring.AddShard("shard-1")

	const n = 2000
	keys := make([]string, n)
	before := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("user-%d", i)
		before[i], _ = ring.ShardFor(keys[i])
	}

	ring.AddShard("shard-2")

	moved := 0
	for i, k := range keys {
		after, _ := ring.ShardFor(k)
		if after != before[i] {
			moved++
		}
	}

	// Adding a 3rd shard to 2 should move roughly 1/3 of keys, not all of them.
	if moved > n/2 {
		t.Errorf("expected a minority of keys to move after adding a shard, moved %d/%d", moved, n)
	}
}

func TestRingShardForEmptyRing(t *testing.T) {
	ring := NewRing(100)
	if _, ok := ring.ShardFor("anything"); ok {
		t.Fatal("expected ok=false for an empty ring")
	}
}
