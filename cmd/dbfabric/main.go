package main

import (
	"log"
	"time"

	"dbfabric/internal/health"
	"dbfabric/internal/pool"
	"dbfabric/internal/router"
	"dbfabric/internal/shardmap"
)

func main() {
	sm := shardmap.New()
	pm := pool.NewManager()
	rt := router.New(sm)
	hc := health.NewChecker(sm, 2*time.Second)

	// TODO: load config/shards.example.yaml via internal/config
	// TODO: start the listener (internal/proxy) — one goroutine per client conn
	// TODO: run hc.Run(ctx) in the background once heartbeats are implemented
	_ = pm
	_ = rt
	_ = hc

	log.Println("dbfabric: skeleton wired up — listener/router/pool/health-checker are stubs, see README for design")
}
