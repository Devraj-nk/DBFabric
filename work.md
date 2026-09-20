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

## Build order step 2 (part 3): real backend connections (pgx) — queries now actually execute

Added `github.com/jackc/pgx/v5` (+ `pgxpool`) as a dependency (`go get` pulled
it fine); this bumped `go.mod`'s `go` directive from 1.22 to 1.25 since pgx
v5.11.0 requires it — go1.27.1 handles that fine.

- **`internal/pool/pool.go`** rewritten: `Manager` now owns one real
  `*pgxpool.Pool` per node address (was a placeholder `struct{Addr string}`).
  A new `Backend{User, Password, Database, SSLMode}` holds the credentials —
  deliberately uniform across the whole shard fleet (one role/database
  reachable at each shard's `host:port`), documented as the simplifying
  assumption it is. `Backend.dsn()` builds the connection string via
  `net/url` (not string concatenation) so credentials with special
  characters can't corrupt the DSN.
  `pgxpool.New` doesn't dial eagerly — confirmed via `go doc` ("A pool
  returns without waiting for any connections to be established") — so
  `Get`/`Drain` bookkeeping is unit-testable without a live Postgres
  (`internal/pool/pool_test.go`: reuse-same-pool, distinct-pools-per-addr,
  drain-forces-recreate, dsn shape).
- **`internal/config`**: added a required `backend:` section
  (`user`/`password`/`database`/`sslmode`) with its own validation
  (`user`/`database` required); updated `shards.example.yaml` and every
  config test fixture. Caught a test-rot risk while doing this: two existing
  error-path tests (duplicate shard id, missing primary) had no `backend:`
  section, so after adding the backend.user check *before* the shard loop,
  they'd have started failing for the wrong reason (missing backend, not the
  thing they claimed to test) while still reporting green. Fixed by giving
  every fixture a valid `backend:` block unless the test is specifically
  about backend validation.
- **`internal/proxy/query.go`**: `handleQuery` now actually calls
  `backend.Query(ctx, query)` and streams the real result back — real
  `RowDescription` (actual column names from `pgx`'s `FieldDescriptions()`),
  real `DataRow`s, and the real `CommandTag` from Postgres itself (so
  `SELECT`/`INSERT`/`UPDATE` tags are exactly what Postgres reports, no tag
  logic of our own needed). No more synthetic routing-decision row — that
  demo is gone now that there's something real to show instead.
- **`internal/proxy/protocol.go`**: `writeDataRow` now takes `[]any` (what
  `pgx.Rows.Values()` returns) instead of `[]string`, with a `nil` value
  correctly encoded as SQL NULL (length `-1`, no bytes) rather than an
  empty string — those are distinct on the wire. Added `formatPGValue` for
  Postgres-text-format quirks that matter most (`bool` as `t`/`f`, not
  `true`/`false`); documented as not exhaustive for every type (arrays,
  composites, exact date/time formatting) rather than pretending it is.
- Real Postgres errors surface as themselves: `pgErrorResponse` type-asserts
  the error to `*pgconn.PgError` and forwards its actual `Code` (SQLSTATE)
  and `Message` when the error came from Postgres, instead of a generic
  `XX000` + `err.Error()` — found this mattered from manual testing (below),
  not from a design doc: `err.Error()` on a `*pgconn.PgError` already
  contains `"ERROR: ..."`, so passing it through as our message field
  produced a doubled `"ERROR:  ERROR: relation ... does not exist"` in
  `psql`. Fixed and reverified.

**Test-suite design decision:** `go test ./...` stays hermetic — no test
requires a reachable Postgres server, so it runs the same way on any
machine with just Go installed. The proxy-level tests
(`internal/proxy/listener_test.go`) were reworked around that constraint:
since there's no fake "report the routing decision" path left to assert on,
they point the router at addresses that are guaranteed to refuse
connections (briefly `net.Listen` then immediately close, freeing the port)
and assert on the resulting `ErrorResponse` — which, usefully, still names
the address it tried to reach. That proves routing picked the right
node (primary vs. the correct replica) *and* that a real connection
failure surfaces as a clean Postgres error rather than a hang or crash,
without needing a real database in CI.

**Real-backend verification was done manually, once, not as an automated
test** — spinning up a whole Postgres cluster on every `go test` run would
make the suite slow and environment-dependent, but the actual forwarding
path needed proving against something real, not just against connection
refusals. Used `initdb`/`pg_ctl` (bundled with the `psql` install already on
this machine) to create a **disposable, trust-auth, custom-port** Postgres
instance in the scratch dir — deliberately not touching the existing
password-protected `postgresql-x64-18` Windows service. Verified with real
`psql`:
- Mixed-type single row (`int`, `text`, `bool`, a date) — confirmed `bool`
  renders as `t`, not `true`.
- Multi-row result (`generate_series`) — confirmed the DataRow loop over
  multiple rows.
- A real Postgres error (`SELECT * FROM no_such_table`) — confirmed the
  client sees Postgres's actual message, cleanly, after the `pgErrorResponse`
  fix.
- Two queries in one `psql` session — confirmed the connection stays usable
  after both a successful and a failing query (ReadyForQuery discipline
  from the previous step holds under real load).

Instance was stopped (`pg_ctl stop`) and its data directory removed
afterward; the throwaway single-shard config used for this test was deleted
too — nothing from this verification is left in the repo.

**Next:** `internal/health`'s heartbeat loop is the last major stub — right
now `BoundedStaleness` always falls back to primary and there's no
failover, because nothing is populating `Node.LagMS` or detecting a dead
primary. That's build-order step 3.

## README: documented the proxy's own SPOF/scaling gap

Added a "Known limitations / future extensions" section to the README after
a direct question about it: the proxy process itself isn't resilient the
way the database tier is — one instance is both a throughput ceiling and a
single point of failure, and fixing that (N stateless replicas behind an
L4 load balancer) needs the shard map externalized to etcd/Consul first,
which `shardmap.Map`'s doc comment already flagged but isn't built. Called
out as a distinct, more conventional problem than the routing/failover
logic this project actually explores, not something to fix as part of the
current build order.

## Build order step 3: health checker — LagMS, failover, real end-to-end verified

Implemented `internal/health`'s heartbeat loop for real, closing the last
major stub. This also unblocked two things that were previously stuck
waiting on it: `BoundedStaleness` routing (needed real `LagMS`) and any
notion of failover at all.

- **`internal/shardmap`**: added `Map.All()` (snapshot of every shard, for
  the checker's sweep) and a first test file for the package (it had none
  before, despite being load-bearing) — `Get`/`Set`/`All` behavior, and
  specifically that `Set` replaces rather than lets a caller mutate a
  `*Shard` in place, since that contract is what makes concurrent
  read-while-updating safe.
- **`internal/health/checker.go`**: `Checker.Run` ticks on `interval` and
  sweeps every shard from `shardMap.All()`. Per node, `pingNode` runs a
  bounded-timeout (`interval/2`) query — `SELECT 1` for a primary,
  `SELECT ... pg_last_xact_replay_timestamp() ...` for a replica's lag — and
  tracks consecutive misses per address to derive status: 1 miss →
  `SUSPECT`, 3 → `DOWN` (fixed constants for now, not config-driven — noted
  as a TODO). Critically, `pingNode` never mutates the `Node` it was
  handed; it returns a full replacement and the caller `Set`s a whole new
  `*Shard` back — required by `shardmap.Map`'s contract, and the reason
  this doesn't need its own extra locking. A failed ping keeps the node's
  last-known `LagMS` instead of zeroing it, so a dead replica doesn't look
  artificially fresh to bounded-staleness routing.
- **`internal/health/failover.go`**: `Controller.Promote` picks the
  least-lagged non-DOWN replica, swaps it in as primary via one
  `shardMap.Set` (atomic from a reader's perspective — no half-updated
  shard is ever visible), and drains the old primary's pool via
  `pool.Manager.Drain` so in-flight/new queries against it fail fast rather
  than hang or get silently retried (the exact concern the README's
  connection-pooling section calls out). Explicitly does **not** implement
  the split-brain quorum guard the README's failover-detection section
  flags — there's only one proxy instance (see the SPOF section just
  added above), so there's no second vantage point to quorum against yet;
  documented as a TODO tied to that limitation rather than silently
  skipped. Also doesn't perform a real PostgreSQL-level promotion
  (`pg_promote()`/orchestrator call) — this updates routing state only, and
  says so in the doc comment.
- **`internal/router`**: `Resolve` gained a `maxLagMS` parameter and both
  `Eventual` and `BoundedStaleness` now actually filter on health —
  `pickReplica` excludes any replica with `Status == StatusDown` (a real
  latent gap closed here: before this step, `Eventual` had no health
  awareness at all and could route to a replica already known dead) and,
  for bounded staleness, also excludes anything over the lag ceiling,
  falling back to primary when nothing qualifies.
- **`internal/proxy/query.go`**: extended the `-- consistency=` hint to
  accept an optional `:<ms>` suffix for bounded staleness (e.g.
  `bounded_staleness:500`), defaulting to a fixed `defaultMaxLagMS` (1000ms,
  not configurable yet) when omitted.
- **`cmd/dbfabric/main.go`**: the checker is now actually started
  (`go hc.Run(ctx)`) instead of being constructed and discarded.

**Tests** (all hermetic — `go test ./...` still needs no reachable
Postgres, same design decision as the previous step):
- `internal/health/failover_test.go`: `selectPromotionCandidate` as a pure
  function (lowest lag wins, DOWN replicas skipped, none-qualify cases),
  and `Controller.Promote` against a real `shardmap.Map`/`pool.Manager`
  with no network involved — success (right replica promoted, old primary
  gone from the replica list, pool actually drained — checked by
  confirming `pool.Manager.Get` returns a *different* pointer afterward),
  unknown shard, and no-healthy-replica error paths.
- `internal/health/checker_test.go`: reuses the same "briefly listen then
  close" trick from the proxy tests to get addresses that refuse
  connections fast, so miss-counting and the `SUSPECT`→`DOWN` transition
  are testable without a real backend. Also confirms the checker's
  failover *trigger* wires through to `Controller.Promote` correctly (its
  error path included) and that an already-DOWN shard doesn't drift or
  misbehave under repeated sweeps.
- Known, accepted gap in automated coverage: a *successful* promotion
  driven by the checker itself (as opposed to `Controller.Promote` called
  directly) needs a real reachable replica to ping successfully, which
  needs a real Postgres — not something the hermetic suite exercises. That
  path was verified manually instead (next).
- `internal/router/router_test.go`: new cases for skipping DOWN replicas,
  falling back to primary when all replicas are DOWN, preferring the
  low-lag replica under a lag ceiling, and falling back when none qualify.

**Real end-to-end failover, verified manually against two disposable
Postgres instances** (not real streaming replicas — two independent
`initdb`'d standalone instances standing in for primary/replica, since
setting up genuine WAL streaming replication was more ceremony than this
verification needed; each tagged with a distinguishing `SELECT
'primary-instance'` / `'replica-instance'` marker row so responses could be
told apart):
1. Configured a single shard with `primary=…:55501`, `replicas=[…:55502]`,
   `health_check_interval: 1s`. Started `dbfabric`, confirmed a strong-
   consistency query through the proxy hit the primary.
2. `pg_ctl -m immediate stop` on the primary instance — simulating a hard
   crash, not a graceful shutdown.
3. ~3 seconds later (3 missed 1s heartbeats, exactly matching
   `downAfterMisses`), the server log showed, unprompted:
   ```
   health: shard "shard-0" primary 127.0.0.1:55501 confirmed DOWN after 3 misses, promoting a replica
   health: shard "shard-0" failed over: primary 127.0.0.1:55501 -> 127.0.0.1:55502 (old primary's pool drained)
   ```
4. A strong-consistency query through the proxy now returned the
   *replica*-instance's marker row — proving the router picked up the
   promoted primary on its very next `Resolve` (shard map reads are always
   fresh, never cached, so this needed no extra wiring).
5. An eventual-consistency query also returned the replica-instance's
   marker — correct, since the replica list is now empty after promotion,
   so `pickReplica` falls back to the (new) primary.

Both instances stopped and their data directories removed afterward; the
throwaway failover-test config was deleted too.

**What's left**: split-brain quorum guard (blocked on there being only one
proxy instance — see the SPOF section), real PostgreSQL-level promotion
(`pg_promote()`), config-driven miss thresholds and default lag ceiling,
and parallelizing node pings within a sweep (currently sequential — fine at
today's scale, flagged as a TODO). Build order's remaining item is step 5,
the chaos test, which this session's manual failover verification is
already a version of — a more formal/repeatable harness for it would be
the natural next thing to build.

## Build order step 5 and the "what's left" list: implemented

Everything from the previous entry's "what's left" list is now done:
config-driven thresholds, parallel pings, real `pg_promote()`, a split-brain
guard, and the chaos harness. (One item changed shape: the guard could not be a
quorum *across proxies* because there is still exactly one, so it uses the
replicas as witnesses instead — see below.) The harness turned out to be the
most valuable piece: it found bugs in code I had already declared working. They
are written up honestly below, including where my own earlier claims were wrong.

### What was built

- **Config** (`internal/config`): optional `health:` block
  (`suspect_after_misses`, `down_after_misses`, `pg_promote`, `quorum_guard`)
  and `routing.default_max_lag_ms`, with defaults and validation
  (`suspect >= 1`, `down >= suspect`, lag `>= 0`). Optional fields are pointers
  in the raw struct so an explicit `false`/`0` is distinguishable from
  "omitted" — otherwise `quorum_guard: false` would silently become the `true`
  default. Tested, including that case.
- **`internal/app`**: the wiring that lived inline in `main.go`, extracted so
  `main` and the chaos harness build the proxy identically. `Run` also stops the
  checker and closes every pool on shutdown (new `pool.Manager.Close`).
- **Health checker** (`internal/health/checker.go`):
  - Shards are checked in parallel, and within a shard the primary and each
    replica are pinged in parallel (was: sequential). A per-shard *busy guard*
    means a shard whose previous check is still running — typically a slow
    failover — is skipped by later sweeps instead of overlapped; that guard is
    also what keeps "replace the shard's whole entry" free of lost updates now
    that more than one goroutine is around. Other shards are never delayed by
    it.
  - `Run` sweeps once immediately, then on each tick, each sweep in its own
    goroutine so a slow one can't delay the next tick.
  - **Failover now retries while the primary stays DOWN.** The previous version
    fired only on the up-to-DOWN *transition*, so a promotion that was vetoed,
    or found no healthy replica, was never attempted again. Repeated identical
    failures are logged once. A primary that answers again (healed partition)
    cancels the attempt.
- **Failover controller** (`failover.go`): guard, then `pg_promote()`, then the
  shard-map swap, then drain — in that order; on any failure the shard map is
  untouched.
  - *Guard:* each non-DOWN replica is asked whether its WAL receiver is still
    `streaming` (`pg_stat_wal_receiver`). Promote only if a strict majority of
    the replicas that *answered* cannot see the primary; ties and no answers
    veto; unreachable replicas abstain. (When unsure, don't make a second
    primary.)
  - *`pg_promote(true, 20)`:* SQLSTATE `55000` ("recovery is not in progress")
    is treated as success, making it idempotent.
- **A seam for tests** (`backends.go`): a 5-method `backends` interface (`Ping`,
  `ReplicationLagMS`, `WalReceiverStreaming`, `PgPromote`, `Drain`) over the pgx
  pools. It exists so the checker/controller logic — miss counting,
  reset-on-success, veto/retry, promotion ordering, parallelism — now has
  **hermetic success-path tests** against a fake. The previous entry had to
  leave "a successful promotion driven by the checker" to manual verification;
  it no longer does.

### Bugs found, and what I got wrong

1. **The idle-replica lag formula was wrong (mine, previous step).** I computed
   lag as `now() - pg_last_xact_replay_timestamp()`. On a real replica of an
   idle primary that reads **4566 ms** (measured) although the replica is fully
   caught up — it would have excluded every healthy replica from
   bounded-staleness reads whenever traffic paused. It only looked fine because
   no earlier test had a real replica. Now: `0` when
   `pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn()`, else the timestamp
   difference. Found by probing a hand-built streaming replica *before* writing
   the harness. Limitation kept honest in the README: this is replay lag
   against what the replica has received, not receive lag against the primary.
2. **Premature acknowledgement of column-less statements (mine, previous step;
   caught by the chaos harness).** `handleQuery` skipped the `rows.Next()` loop
   when a statement returned no columns (INSERT/UPDATE), then read `rows.Err()`
   and `rows.CommandTag()` — which pgx documents as valid only after the rows
   are closed. So the proxy sent `CommandComplete` (with an empty tag) *before
   the write had finished*. A live primary that never crashed showed "2
   acknowledged writes missing". **My earlier report that "2 of 1226 acked
   writes were lost to the async-replication window" was an unproven guess and
   was probably this bug.** Fixed by extracting `streamResult` and always
   draining the rows. Regression tests use a fake `pgx.Rows` that reproduces
   the contract (outcome invisible until closed); I confirmed by mutation
   (re-introducing the skip) that they fail against the old behavior with
   exactly the empty-tag / premature-`C` symptom. After the fix: 0 acknowledged
   writes lost, in every run.
3. **The handshake announced too few parameters (mine; caught by the
   harness).** Only `server_version` and `client_encoding` were sent; pgx in
   simple-protocol mode refuses to run any `Query` without
   `standard_conforming_strings=on`. Writes worked only because pgx skips that
   check for argument-less `Exec`. Now a fuller set matching Postgres's defaults
   is announced, with a regression test.
4. **Transactions silently broke atomicity (mine; found while verifying README
   claims, not by the harness).** `BEGIN; INSERT 99; ROLLBACK` left row 99
   *committed*: each query takes a pooled connection, pgxpool destroys a
   connection released mid-transaction, so the BEGIN was lost, the INSERT ran in
   autocommit elsewhere, and the ROLLBACK was a no-op. A silent atomicity
   violation is worse than an error, so transaction-control statements
   (`BEGIN/START/COMMIT/END/ROLLBACK/ABORT/SAVEPOINT/RELEASE`, also behind a
   `-- consistency=` comment) are now refused with an explanatory `0A000` error.
   **Real transaction support (pinning a backend connection per transaction, and
   deciding what failover mid-transaction means) is not built and is a genuine
   design decision — see "open questions".** Also learned in passing: `SET`
   "worked" across two queries only because the pool handed back the same idle
   connection; it is not reliable, and the README now says so.
5. **`pool.Manager.Drain` held its mutex across `pgxpool.Close()`**, which blocks
   until every checked-out connection is returned — one slow query on a dead
   node would have frozen `Get` for every other node. And my comment that
   draining makes in-flight queries "fail fast" was wrong (Close *waits* for
   them). Fixed: detach under the lock, close outside it; `DrainAsync` detaches
   synchronously and closes in the background, which is what failover uses so a
   hung query on the dead primary can't hold up its own replacement. (No test
   can exercise the blocking close without a live connection, so I removed a
   test I had written that could not fail, rather than keep false confidence.)

### Chaos harness (`internal/chaos`, build tag `chaos`)

Real `initdb` + `pg_basebackup` streaming replication; the proxy runs
in-process via `internal/app`; four concurrent at-least-once writers retry an
insert until acknowledged (so loss and duplication are observable). `make
chaos`. Kept out of `go test ./...`, which still needs no database.

- **Partition modelling:** a TCP relay between the proxy and a *live* primary.
  Cutting it makes the primary unreachable to the proxy while it stays up and
  keeps feeding its replica (whose WAL receiver connects directly). Killing the
  primary can't produce that situation.
- **Recovery metric:** longest gap between consecutive acknowledged writes, not
  "time since `pg_ctl stop` returned" — my first cut measured the latter and
  under-reported the stall (the primary is already dying during that call).

Final runs (one Windows machine, localhost, PG 18.4, 500 ms sweep, DOWN@3):

| Scenario | Result |
|---|---|
| Kill primary mid-burst (5 runs) | stall 1.5-2.0 s; **0** acked writes lost of ~2,000 acked pre-failover per run; 0 duplicates; ~110 client-visible errors |
| Kill replica | DOWN in ~1.1-1.3 s; **0** write errors; ~25 eventual reads fail in the detection window, 0 after |
| Partition from live primary, guard on (4 runs) | every attempt vetoed (`1 replica(s) still see primary, 0 cannot`); no promotion; writes stalled ~5.6 s and resumed after heal; 0 lost, 0 duplicates |
| Same, guard off | split brain reproduced: `{-1:1,-2:0}` on the old primary vs `{-1:0,-2:1}` on the promoted node |

Not proven: loss to asynchronous replication at crash time. Localhost lag is
sub-millisecond so it did not appear; it remains possible in principle.

**Two test-quality findings worth remembering:**
- A flaky health test (`SlowFailoverDoesNotBlockOtherShards...`) failed roughly
  once in a few dozen runs. The code was right and the *test* raced: it waited
  on a lag value already set by earlier sweeps, so it proceeded before the
  blocked sweep's shard-b check had finished, and the busy guard correctly
  skipped it. Fixed with a fresh sentinel value plus an explicit "not busy"
  wait; then 500 consecutive clean iterations. Found only because I stress-ran
  (`-count=30`) instead of trusting one green run.
- A helper hang: `pg_ctl start` output piped into anything deadlocks, because
  the daemonized server inherits the pipe and holds it open until it exits —
  `exec.CombinedOutput` would hang identically. The harness sends tool output to
  files under the test's temp dir (a first version leaked `pgtool-*.log` into
  `%TEMP%`, since Windows won't delete a file the server still holds open).

### Not done / caveats

- **`go test -race` could not be run**: the installed Go is `windows/386` and the
  race detector doesn't support it. I reviewed the sharing by hand (per-node
  results written to distinct slice indexes and read after `WaitGroup.Wait`;
  shared maps behind `c.mu`), but that is not the same. Installing the 64-bit Go
  toolchain would enable it.
- Failover is **routing-state failover with sharp edges**, all now stated in the
  README: no fencing of the old primary; the old primary isn't re-added and
  surviving replicas aren't repointed; the guard depends on replicas noticing a
  dead primary (up to `wal_receiver_timeout`, 60 s, under a *silent* partition);
  reads sent to a dead replica before its DOWN mark are not retried.
- **The proxy performs no authentication** and doesn't terminate TLS — the
  README now leads its limitations with this.
- Metrics are still a placeholder package.
- The README "Layout" section you added was stale (said "stubs", "stdlib
  only"); I corrected it to match the code.

### Open questions for the owner

1. **Transactions.** Pin a backend connection per client transaction (proper,
   bigger), or keep refusing them (current)? Pinning also forces decisions on
   failover mid-transaction and on which node a transaction may use.
2. **Authentication.** Is this meant to stay a trusted-network component, or
   should it authenticate clients?
3. **Extended query protocol.** Most drivers default to Parse/Bind/Execute; only
   the simple protocol works today. Supporting it is the largest remaining gap
   for real-world clients.
