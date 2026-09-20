// Package app wires the proxy's components together from a loaded config.
// cmd/dbfabric runs it as a process; the chaos harness runs it in-process
// so it can inspect the shard map while injecting failures.
package app

import (
	"context"
	"errors"
	"log"

	"dbfabric/internal/config"
	"dbfabric/internal/health"
	"dbfabric/internal/pool"
	"dbfabric/internal/proxy"
	"dbfabric/internal/router"
	"dbfabric/internal/shardmap"
)

// App is a fully wired proxy: shard map, router, connection pools, health
// checker and client listener.
type App struct {
	ShardMap *shardmap.Map
	Router   *router.Router
	Pools    *pool.Manager
	Checker  *health.Checker
	Listener *proxy.Listener
}

// New builds an App from cfg and registers its shards. Nothing runs until
// Run is called.
func New(cfg *config.Config) *App {
	sm := shardmap.New()
	rt := router.New(sm)
	pm := pool.NewManager(pool.Backend{
		User:     cfg.Backend.User,
		Password: cfg.Backend.Password,
		Database: cfg.Backend.Database,
		SSLMode:  cfg.Backend.SSLMode,
	})

	for _, s := range cfg.Shards {
		shard := &shardmap.Shard{ID: s.ID, Primary: shardmap.Node{Addr: s.Primary}}
		for _, addr := range s.Replicas {
			shard.Replicas = append(shard.Replicas, shardmap.Node{Addr: addr})
		}
		rt.AddShard(shard)
	}

	return &App{
		ShardMap: sm,
		Router:   rt,
		Pools:    pm,
		Checker: health.NewChecker(sm, pm, health.Options{
			Interval:           cfg.HealthCheckInterval,
			SuspectAfterMisses: cfg.Health.SuspectAfterMisses,
			DownAfterMisses:    cfg.Health.DownAfterMisses,
			PgPromote:          cfg.Health.PgPromote,
			QuorumGuard:        cfg.Health.QuorumGuard,
		}),
		Listener: proxy.NewListener(cfg.ListenAddr, rt, pm, cfg.Routing.DefaultMaxLagMS),
	}
}

// Run starts the health checker in the background and serves clients until
// ctx is cancelled or the listener fails. It stops the checker and closes
// every backend pool before returning.
func (a *App) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	checkerDone := make(chan struct{})
	go func() {
		defer close(checkerDone)
		if err := a.Checker.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("health checker stopped: %v", err)
		}
	}()

	err := a.Listener.Run(ctx)

	cancel()
	<-checkerDone
	a.Pools.Close()
	return err
}
