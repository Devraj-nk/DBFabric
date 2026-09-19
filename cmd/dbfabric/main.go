package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"

	"dbfabric/internal/config"
	"dbfabric/internal/health"
	"dbfabric/internal/pool"
	"dbfabric/internal/proxy"
	"dbfabric/internal/router"
	"dbfabric/internal/shardmap"
)

func main() {
	configPath := flag.String("config", "config/shards.example.yaml", "path to the shard map config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("loading config: %v", err)
	}

	sm := shardmap.New()
	rt := router.New(sm)
	pm := pool.NewManager()
	_ = health.NewChecker(sm, cfg.HealthCheckInterval) // TODO: run hc.Run(ctx) once heartbeats are implemented

	for _, s := range cfg.Shards {
		shard := &shardmap.Shard{ID: s.ID, Primary: shardmap.Node{Addr: s.Primary}}
		for _, addr := range s.Replicas {
			shard.Replicas = append(shard.Replicas, shardmap.Node{Addr: addr})
		}
		rt.AddShard(shard)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	lis := proxy.NewListener(cfg.ListenAddr, rt, pm)
	log.Printf("dbfabric: listening on %s (%d shard(s) loaded from %s)", cfg.ListenAddr, len(cfg.Shards), *configPath)
	if err := lis.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("listener: %v", err)
	}
	log.Println("dbfabric: shut down")
}
