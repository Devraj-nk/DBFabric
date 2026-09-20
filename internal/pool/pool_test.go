package pool

import (
	"context"
	"strings"
	"testing"
)

// These tests don't need a reachable Postgres server: pgxpool.New
// only parses the connection string and prepares the pool without
// dialing — the actual connection attempt happens on first use.

func TestGetCreatesAndReusesPool(t *testing.T) {
	m := NewManager(Backend{User: "app", Database: "appdb"})

	p1, err := m.Get(context.Background(), "127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	p2, err := m.Get(context.Background(), "127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	if p1 != p2 {
		t.Error("Get returned a different pool for the same address")
	}
}

func TestGetDistinctAddrsGetDistinctPools(t *testing.T) {
	m := NewManager(Backend{User: "app", Database: "appdb"})

	p1, err := m.Get(context.Background(), "127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	p2, err := m.Get(context.Background(), "127.0.0.1:2")
	if err != nil {
		t.Fatal(err)
	}
	if p1 == p2 {
		t.Error("Get returned the same pool for two different addresses")
	}
}

func TestDrainForcesNewPoolOnNextGet(t *testing.T) {
	m := NewManager(Backend{User: "app", Database: "appdb"})

	p1, err := m.Get(context.Background(), "127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	m.Drain("127.0.0.1:1")

	p2, err := m.Get(context.Background(), "127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	if p1 == p2 {
		t.Error("Get returned the drained pool instead of creating a new one")
	}
}

func TestDrainAsyncDetachesBeforeReturning(t *testing.T) {
	m := NewManager(Backend{User: "app", Database: "appdb"})

	p1, err := m.Get(context.Background(), "127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	m.DrainAsync("127.0.0.1:1")

	// The close may still be running in the background, but the node must
	// already be unroutable: the very next Get builds a fresh pool.
	p2, err := m.Get(context.Background(), "127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	if p1 == p2 {
		t.Error("DrainAsync returned while the old pool was still attached")
	}
}

func TestDrainUnknownAddrIsANoop(t *testing.T) {
	m := NewManager(Backend{User: "app", Database: "appdb"})
	m.Drain("127.0.0.1:9999") // must not panic
}

func TestCloseEmptiesEveryPool(t *testing.T) {
	m := NewManager(Backend{User: "app", Database: "appdb"})
	p1, err := m.Get(context.Background(), "127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Get(context.Background(), "127.0.0.1:2"); err != nil {
		t.Fatal(err)
	}

	m.Close()

	// After Close, Get builds a fresh pool rather than returning a closed one.
	p1again, err := m.Get(context.Background(), "127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	if p1 == p1again {
		t.Error("Get returned a pool that Close should have removed")
	}
}

func TestBackendDSNShape(t *testing.T) {
	b := Backend{User: "app", Password: "p@ss/word", Database: "appdb", SSLMode: "require"}
	dsn := b.dsn("127.0.0.1:6543")

	for _, want := range []string{"postgres://", "127.0.0.1:6543", "/appdb", "sslmode=require"} {
		if !strings.Contains(dsn, want) {
			t.Errorf("dsn %q does not contain %q", dsn, want)
		}
	}
}

func TestBackendDSNDefaultsSSLModeToDisable(t *testing.T) {
	b := Backend{User: "app", Database: "appdb"}
	dsn := b.dsn("127.0.0.1:6543")
	if !strings.Contains(dsn, "sslmode=disable") {
		t.Errorf("dsn %q should default sslmode to disable", dsn)
	}
}
