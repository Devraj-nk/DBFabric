package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"time"

	"dbfabric/internal/app"
	"dbfabric/internal/config"
	"dbfabric/internal/simulator"
)

func main() {
	configPath := flag.String("config", "config/shards.example.yaml", "path to the shard map config file")
	uiAddr := flag.String("ui", "", "serve the routing simulator UI on this address (for example :8080)")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("loading config: %v", err)
	}

	if *uiAddr != "" {
		server := simulator.NewServer(cfg, *uiAddr)
		go func() {
			if err := server.Run(); err != nil {
				log.Printf("simulator UI stopped: %v", err)
			}
		}()
		log.Printf("dbfabric: simulator UI at http://%s", *uiAddr)
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
		defer stop()
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	log.Printf("dbfabric: listening on %s (%d shard(s) loaded from %s)", cfg.ListenAddr, len(cfg.Shards), *configPath)
	if err := app.New(cfg).Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("dbfabric: %v", err)
	}
	log.Println("dbfabric: shut down")
}
