# DBFabric — Working Notes

## Project scaffolding

Set up the base Go module structure before Go itself is installed locally
(all hand-written text files — no `go mod init`/toolchain needed to create
them, just to build later).

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

## Build order step 1: static shard map, route by hash

Go is now installed (`go1.27.1 windows/386`); confirmed `go build ./...` and
`go vet ./...` were clean on the scaffold before adding real logic.

Implemented the first two stubs for real:

- **`internal/router/hash.go` — `Ring`**: a real consistent-hash ring
  (`hash/crc32` + a sorted `[]uint32` of vnode hashes, 100 vnodes/shard by
  default). `AddShard`/`RemoveShard` mutate the ring; `ShardFor` does a
  `sort.Search` to the first vnode at-or-after `hash(key)`, wrapping around.
  Deliberately no third-party deps — `crc32` is stdlib and good enough for
  routing (not a security context).
- **`internal/router/router.go` — `Router.Resolve`**: takes `(shardKey,
  Consistency)` and returns a `shardmap.Node`. `Strong` → primary always.
  `Eventual` → `hash(key) % len(replicas)` (deterministic, no per-shard
  round-robin counter needed) falling back to primary if a shard has no
  replicas yet. `BoundedStaleness` → falls back to primary for now with a
  TODO, since it needs `Node.LagMS` from the health checker (build-order
  step 3, not built yet) — that's the correct/safe default, not a shortcut.
- Split `Router.AddShard` (topology change: touches both the shard map and
  the ring) from `shardMap.Set` (value update, e.g. a future failover
  promotion swapping which node is primary) — the ring must NOT be touched
  by a primary/replica change, only by a shard being added or removed,
  otherwise every key's routing would reshuffle on every failover.

**Tests added** (`internal/router/hash_test.go`, `router_test.go`):
even distribution across 3000 synthetic keys, same-key routing is stable
across repeated calls, adding a 3rd shard to 2 moves a minority of keys
(not all of them — the actual point of consistent hashing), empty-ring
behavior, and `Router.Resolve` correctness for all three consistency levels
plus the no-shards/unknown-consistency error paths.

`go build ./...`, `go vet ./...`, and `go test ./...` all pass. Wired a
static 2-shard map into `cmd/dbfabric/main.go` (hardcoded, matching
`config/shards.example.yaml` — real YAML loading is still a stub) and had it
print `strong`/`eventual` resolution for a few sample keys as a smoke test;
confirmed via `go run ./cmd/dbfabric`.

**Next (build-order step 2):** replicas + read/write splitting already have
a basic form (`Eventual` picks a replica) — the real remaining step-2 work is
wiring `internal/config.Load` to actually parse
`config/shards.example.yaml` instead of the hardcoded shards in `main.go`,
and starting `internal/proxy.Listener` so routing decisions apply to real
connections instead of a print-loop demo.

## Build order step 2 (part 1): real config loading

Added the first third-party dependency: `gopkg.in/yaml.v3` (network fetch via
`go get` worked fine in this environment). `go.mod` now has it as a direct,
non-indirect require after `go mod tidy`.

- **`internal/config/config.go`**: `Load(path)` reads + parses
  `config/shards.example.yaml`-shaped YAML into a `*Config`. Unmarshal target
  is a private `rawConfig`/`rawShardConfig` pair, not the public `Config`
  directly — `yaml.v3` can't unmarshal a duration string (`"2s"`) straight
  into a `time.Duration`, so `HealthCheckInterval` stays a string on the raw
  struct and gets run through `time.ParseDuration` by hand.
- Validates on load rather than deferring to callers: rejects an empty
  shard list, a shard with no `id`, a duplicate `id`, or a shard with no
  `primary`. Fails fast at startup instead of the router discovering a
  half-broken shard map later.
- `internal/config` deliberately doesn't import `internal/shardmap` — it
  hands back plain `ShardConfig{ID, Primary, Replicas []string}` and
  `main.go` does the conversion to `shardmap.Shard`/`shardmap.Node`. Keeps
  config parsing decoupled from the shard-map's internal representation.
- `cmd/dbfabric/main.go`: replaced the hardcoded 2-shard map with
  `config.Load`, plus a `-config` flag (default
  `config/shards.example.yaml`) so a different shard map can be pointed at
  without a rebuild.

**Tests** (`internal/config/config_test.go`): loads the real
`config/shards.example.yaml` and asserts its actual values (keeps the test
honest against that file, not a copy of it), plus one failure test per
validation rule (missing file, bad duration, no shards, duplicate id,
missing primary), using `t.TempDir()` + hand-written YAML fixtures.

`go mod tidy`, `go build ./...`, `go vet ./...`, `go test ./...` all clean.
`go run ./cmd/dbfabric` now prints `loaded 2 shard(s) from
config/shards.example.yaml` and the same routing output as before (the
shard topology didn't change, just where it comes from).

**Still open for step 2 / step 3:** `internal/proxy.Listener` still only
does a bare TCP accept loop — no Postgres wire protocol handshake or query
parsing yet, so routing decisions still don't apply to a real client
connection. That's a substantially bigger, separate chunk of work
(handshake + simple-query-protocol parsing at minimum) and deserves its own
step rather than a quick bolt-on. `internal/health` and
`internal/pool`'s pgx-backed connections are still stubs too.

## Build order step 2 (part 2): real Postgres wire-protocol handshake + query routing

Implemented enough of the Postgres v3 frontend/backend protocol
(`internal/proxy/protocol.go`) for a real client to connect and issue
queries — not a general driver, just the startup handshake plus the simple
query subprotocol:

- Pre-auth framing (`readStartupParams`, `readUntyped`): handles
  SSLRequest/GSSENCRequest by answering `'N'` (no TLS termination here) and
  looping until the real StartupMessage arrives; parses its null-terminated
  key/value param list.
- Post-auth framing (`readMessage`/`writeMessage`): the standard
  1-byte-type + 4-byte-length + payload envelope every later message uses.
- Just enough backend messages to complete a handshake and answer a simple
  query: `AuthenticationOk`, `ParameterStatus`, `BackendKeyData`,
  `ReadyForQuery`, `RowDescription`, `DataRow`, `CommandComplete`,
  `ErrorResponse`. Auth is unconditional trust — no password/SASL, matching
  where the project actually is (no user/credential model yet).

**Routing hint conventions** (`internal/proxy/query.go`), called out as
interim, not real SQL parsing, since general SQL doesn't carry a shard key:
- Shard key comes from the startup param `application_name` (falls back to
  `user`).
- Consistency level comes from a required-format leading comment,
  `-- consistency=eventual`, defaulting to `strong` if absent.
Both are documented in code as placeholders — a real deployment would need
an agreed hint mechanism (dedicated startup param, schema convention, etc.),
same as the README already flags cross-shard queries as "the hard part."

**What a query actually gets right now**: since `internal/pool` has no real
backend connections yet (still a stub — no pgx), the proxy does NOT forward
the query anywhere. `handleQuery` resolves shard key + consistency through
the real `Router` built in steps 1-2 and reports the decision back as a
one-row result set (`shard_key`, `consistency`, `routed_to`) instead of
executing anything. Once `internal/pool` is backed by pgx, this is the
exact point where forwarding replaces the report.

**A real bug caught only by manual testing, not the test suite:** the
initial version never sent a trailing `ReadyForQuery` after
`CommandComplete`. The `net.Pipe`-based test suite passed anyway because
the test client didn't wait for one either — it's a case where the test and
the code shared the same wrong assumption. Only testing against a real
`psql` client (available at `C:\Program Files\PostgreSQL\18\bin\psql`)
surfaced it: `psql` hung indefinitely after the query, because the simple
query protocol requires ReadyForQuery after every command before a client
will consider it done. Fixed by writing ReadyForQuery once per message
handled (success or error) in the connection loop, and added an assertion
for it in the test. Lesson: a synthetic in-memory protocol test can pass
while encoding the same wrong assumption as the code — worth a real-client
smoke test for anything implementing an existing wire protocol.

**Verified end to end**, not just via `go test`:
1. `go build`, `go vet`, `go test ./...` — all clean, including new tests in
   `internal/proxy/protocol_test.go` (SSL negotiation, startup param
   parsing, message round-trips) and `listener_test.go` (full handshake +
   query over `net.Pipe`).
2. Built the real binary, ran it, and connected with actual `psql` over TCP:
   `psql "host=127.0.0.1 port=5433 dbname=app user=app
   application_name=user-42 sslmode=disable" -c "-- consistency=eventual
   SELECT 1"` returned the expected routing row, and a second query on the
   same session (default strong consistency, then an eventual one) also
   worked — confirming the connection stays usable across multiple queries,
   not just one.

`main.go` now actually starts `proxy.Listener.Run` (with
`signal.NotifyContext(os.Interrupt)` for graceful shutdown) instead of
printing a routing demo and exiting.

**Next:** `internal/pool` needs real backend connections (pgx) so
`handleQuery` can forward instead of report — that's the natural next step
now that both ends of the "route correctly" problem (client-facing
protocol, router) are real. `internal/health`'s heartbeat loop and
`internal/config`'s shard-map-only-does-nothing-with-health-yet are still
open after that.
