# DBFabric — Working Notes

## Project scaffolding

Set up the base Go module structure before Go itself is installed locally
(all hand-written text files — no `go mod init`/toolchain needed to create
them, just to build later).

**Layout**, one package per HLD component from the README:

```
DBFabric/
├── go.mod                          module dbfabric, go 1.22 — stdlib only, no deps yet
├── Makefile                        build/run/test/tidy targets
├── .gitignore
├── cmd/dbfabric/main.go            wires everything together (stub)
├── config/shards.example.yaml      shape internal/config will eventually parse
└── internal/
    ├── shardmap/shardmap.go        routing table: shard ID → {primary, replicas, lag}
    ├── router/router.go, hash.go   shard-key resolution + consistency routing (stubs)
    ├── pool/pool.go                per-node connection pool manager (Get/Drain implemented)
    ├── health/checker.go, failover.go   heartbeat loop + failover controller (stubs)
    ├── proxy/listener.go           TCP accept loop, one goroutine/conn (accept loop works, query handling is a stub)
    └── metrics/metrics.go          TODO for prometheus wiring
```

**What's real vs. stubbed:**
- Real logic: `shardmap.Map` (RWMutex-guarded get/set), `pool.Manager`
  (get-or-create / drain), `proxy.Listener.Run` (accept loop with
  context-cancellation shutdown, one goroutine per connection).
- Stubs (`panic("not implemented")` + TODO comments): consistent hashing
  (`router.ShardFor`), consistency-level resolution (`router.Router.Resolve`),
  heartbeat body (`health.Checker.Run`'s ticker case), failover promotion
  (`health.Controller.Promote`), YAML config loading (`config.Load`),
  Prometheus metrics (`metrics` package is empty).
- `go.mod` deliberately has no third-party requires yet — everything used so
  far is stdlib (`net`, `sync`, `context`, `time`). `pgx`/`pgxpool`, a YAML
  parser, and `prometheus/client_golang` get added via `go get` once each
  stubbed area is implemented, so there's nothing to fetch before Go is even
  installed.

**Next step once Go is installed:** `make build` (or `go build ./...`) as a
first sanity check — nothing has been compiled yet, so this only verifies the
hand-written files are syntactically correct Go.

**Note:** this file was found empty on disk before this entry (previously had
the Python-vs-Go tech-stack discussion, HLD, and data-flow notes) — that
content now lives only in the README (§Tech stack, §High-level design,
§Data flow), not here. Decided not to reconstruct the discursive notes here,
only append from this point forward.
