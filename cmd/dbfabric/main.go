package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"

	"dbfabric/internal/app"
	"dbfabric/internal/config"
)

func main() {
	configPath := flag.String("config", "config/shards.example.yaml", "path to the shard map config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("loading config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	log.Printf("dbfabric: listening on %s (%d shard(s) loaded from %s)", cfg.ListenAddr, len(cfg.Shards), *configPath)
	if err := app.New(cfg).Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("dbfabric: %v", err)
	}
	log.Println("dbfabric: shut down")
}
