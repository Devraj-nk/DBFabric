package simulator

import (
	"testing"

	"dbfabric/internal/config"
	"dbfabric/internal/router"
	"dbfabric/internal/shardmap"
)

func testConfig() *config.Config {
	return &config.Config{
		ListenAddr: ":5433",
		Routing:    config.RoutingConfig{DefaultMaxLagMS: 1000},
		Shards: []config.ShardConfig{{
			ID: "shard-0", Primary: "primary:5432", Replicas: []string{"replica-a:5432", "replica-b:5432"},
		}},
	}
}

func TestRouteUsesConsistencyAndLag(t *testing.T) {
	sim := New(testConfig())
	eventual, err := sim.Route(routeRequest{ShardKey: "user-42", Consistency: string(router.Eventual)})
	if err != nil || eventual.Role != "replica" {
		t.Fatalf("eventual route = %+v, err %v; want replica", eventual, err)
	}
	if err := sim.Action(actionRequest{Action: "lag", Shard: "shard-0", Node: eventual.Node, LagMS: 2000}); err != nil {
		t.Fatal(err)
	}
	otherReplica := "replica-a:5432"
	if eventual.Node == otherReplica {
		otherReplica = "replica-b:5432"
	}
	if err := sim.Action(actionRequest{Action: "lag", Shard: "shard-0", Node: otherReplica, LagMS: 2000}); err != nil {
		t.Fatal(err)
	}
	bounded, err := sim.Route(routeRequest{ShardKey: "user-42", Consistency: string(router.BoundedStaleness), MaxLagMS: 1000})
	if err != nil || bounded.Role != "primary" {
		t.Fatalf("bounded route = %+v, err %v; want primary fallback", bounded, err)
	}
}

func TestPromotionChoosesHealthyLeastLaggedReplica(t *testing.T) {
	sim := New(testConfig())
	if err := sim.Action(actionRequest{Action: "lag", Shard: "shard-0", Node: "replica-a:5432", LagMS: 500}); err != nil {
		t.Fatal(err)
	}
	if err := sim.Action(actionRequest{Action: "lag", Shard: "shard-0", Node: "replica-b:5432", LagMS: 50}); err != nil {
		t.Fatal(err)
	}
	if err := sim.Action(actionRequest{Action: "promote", Shard: "shard-0"}); err != nil {
		t.Fatal(err)
	}
	shard, ok := sim.shardMap.Get("shard-0")
	if !ok || shard.Primary.Addr != "replica-b:5432" || shard.Primary.Status != shardmap.StatusUp {
		t.Fatalf("promoted shard = %+v; want replica-b UP", shard)
	}
}
