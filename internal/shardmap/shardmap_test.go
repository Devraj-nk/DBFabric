package shardmap

import "testing"

func TestGetMissing(t *testing.T) {
	m := New()
	if _, ok := m.Get("shard-0"); ok {
		t.Fatal("expected ok=false for a shard that was never set")
	}
}

func TestSetThenGet(t *testing.T) {
	m := New()
	s := &Shard{ID: "shard-0", Primary: Node{Addr: "primary-0"}}
	m.Set("shard-0", s)

	got, ok := m.Get("shard-0")
	if !ok {
		t.Fatal("expected ok=true after Set")
	}
	if got != s {
		t.Error("Get did not return the same *Shard passed to Set")
	}
}

func TestSetReplacesRatherThanMutates(t *testing.T) {
	m := New()
	original := &Shard{ID: "shard-0", Primary: Node{Addr: "primary-0"}}
	m.Set("shard-0", original)

	replacement := &Shard{ID: "shard-0", Primary: Node{Addr: "primary-1"}}
	m.Set("shard-0", replacement)

	got, _ := m.Get("shard-0")
	if got != replacement {
		t.Error("Get did not return the replacement *Shard")
	}
	if original.Primary.Addr != "primary-0" {
		t.Error("the original *Shard was mutated instead of being replaced")
	}
}

func TestAllReturnsEverySetShard(t *testing.T) {
	m := New()
	m.Set("shard-0", &Shard{ID: "shard-0"})
	m.Set("shard-1", &Shard{ID: "shard-1"})

	all := m.All()
	if len(all) != 2 {
		t.Fatalf("len(All()) = %d, want 2", len(all))
	}
	seen := map[string]bool{}
	for _, s := range all {
		seen[s.ID] = true
	}
	if !seen["shard-0"] || !seen["shard-1"] {
		t.Errorf("All() = %v, missing an expected shard", all)
	}
}

func TestAllOnEmptyMap(t *testing.T) {
	m := New()
	if all := m.All(); len(all) != 0 {
		t.Errorf("All() on empty map = %v, want empty", all)
	}
}
