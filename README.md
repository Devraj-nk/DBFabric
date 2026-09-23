# DBFabric

Sharded, Replicated Database Proxy

## Core idea

This is a proxy between your application and a set of Postgres/MySQL instances. The app talks only to the proxy; the proxy decides which physical database handles each query.

## Architecture components

### 1. Sharding layer

- Partition data by a shard key (e.g., `user_id`) using consistent hashing so adding/removing shards doesn't reshuffle everything
- Maintain a shard map: `shard_key_range → primary_node`
- Handle cross-shard queries (the hard part) — either reject them, scatter-gather, or restrict your schema to avoid needing them

### 2. Replication awareness

- Each shard = 1 primary + N replicas
- Writes always go to primary; reads can go to replicas (read/write splitting)
- Track replication lag per replica — stale replicas shouldn't serve reads if the app needs read-your-writes consistency

### 3. Failover detection

- Heartbeat/health-check each primary and replica
- On primary failure: promote a replica (or trigger your orchestrator — Patroni-style — to do it), update the shard map, redirect in-flight and new queries
- This is where you'll hit the real distributed systems problem: how do you avoid split-brain if the "dead" primary is actually just network-partitioned from the proxy but alive for clients?

### 4. Connection pooling under failure

- Pool connections per backend node
- When a node fails, drain its pool gracefully, don't leak connections, don't let one slow/dead backend exhaust your total pool (this is basically a mini bulkhead pattern)

### 5. Consistency knobs

- Let the caller choose: strong (read from primary), eventual (read from any replica), bounded-staleness (read from replica only if lag < X ms)
- This is your CAP-theorem-in-practice talking point for interviews

## Tech stack

**Backend: Go.**

The project is fundamentally about getting sharding, replication awareness, failover, and consistency routing *correct* under concurrent access and partial failure, not just working on the happy path. Go's concurrency model fits that directly:

- Goroutines + channels map onto the architecture as-is: a goroutine per client connection, a health-checker that fans heartbeat results into a channel consumed by a single shard-map writer, `select` for racing a query against a timeout.
- `context.Context` gives deadline/cancellation propagation for bounded-staleness reads, draining a pool mid-failover, and heartbeat timeouts — and every backend library (`pgx`, `database/sql`) already speaks it.
- No GIL — real parallelism across cores as connection count grows, on top of the I/O concurrency.
- Direct precedent: Vitess (sharded MySQL proxy with replica routing and failover — the closest real-world sibling to this project) is written in Go.
- Static binary deployment is convenient for a proxy you're going to chaos-test by repeatedly killing and restarting.

Cost: more verbose than a dynamic language (explicit `if err != nil` on the hot path), slower first draft per component. The trade is worth it here because correctness of the concurrent/failure-handling logic is the actual point of the project.

**Core libraries:**
- `net` — TCP listener, one goroutine per connection
- `pgx` / `pgxpool` — Postgres driver + connection pooling
- `context` — deadline/cancellation propagation
- `prometheus/client_golang` — metrics export
- (later) `go.etcd.io/etcd/client/v3` if the shard map needs to be externalized for proxy-level HA

## High-level design

```
                         ┌───────────────────────────────┐
                         │           Clients             │
                         │  (apps speaking Postgres wire │
                         │   protocol, unmodified)       │
                         └───────────────┬───────────────┘
                                         │
                                         ▼
                         ┌──────────────────────────────────┐
                         │            PROXY                 │
                         │                                  │
                         │  ┌───────────────────────────┐   │
                         │  │  Listener / Session Mgr   │   │  data plane
                         │  │  (net.Listener,           │   │
                         │  │   one goroutine/conn)     │   │
                         │  └────────────┬──────────────┘   │
                         │               ▼                  │
                         │  ┌────────────────────────────┐  │
                         │  │        Router              │  │
                         │  │  - extract shard key       │  │
                         │  │  - consistent hash lookup  │  │
                         │  │  - consistency-level logic │  │
                         │  └────────────┬───────────────┘  │
                         │               ▼                  │
                         │  ┌────────────────────────────┐  │
                         │  │   Connection Pool Manager  │  │
                         │  │  (per-node pools, bulkhead │  │
                         │  │   isolation on failure)    │  │
                         │  └────────────┬───────────────┘  │
                         │               │                  │
                         │  ┌────────────┴───────────────┐  │
                         │  │      Shard Map (state)     │  │  control plane
                         │  │  shard_key_range → primary │  │
                         │  │  + replica set + lag info  │  │
                         │  └────────────┬───────────────┘  │
                         │               ▲                  │
                         │  ┌────────────┴───────────────┐  │
                         │  │  Health Checker /          │  │
                         │  │  Failover Controller       │  │
                         │  │  (heartbeats, promotion,   │  │
                         │  │   split-brain guards)      │  │
                         │  └────────────────────────────┘  │
                         │                                  │
                         │  ┌───────────────────────────┐   │
                         │  │  Metrics / Observability  │   │
                         │  │  (Prometheus exporter)    │   │
                         │  └───────────────────────────┘   │
                         └───────────────┬──────────────────┘
                                         │
              ┌──────────────────────────┼──────────────────────────┐
              ▼                          ▼                          ▼
      ┌───────────────┐          ┌───────────────┐          ┌───────────────┐
      │   Shard 1     │          │   Shard 2     │          │   Shard N     │
      │ Primary + N   │          │ Primary + N   │          │ Primary + N   │
      │ Replicas      │          │ Replicas      │          │ Replicas      │
      │ (Postgres)    │          │ (Postgres)    │          │ (Postgres)    │
      └───────────────┘          └───────────────┘          └───────────────┘
```

## Layout
One package per HLD component:

```
DBFabric/
├── go.mod, go.sum
├── Makefile                        build / run / test / chaos / tidy
├── cmd/dbfabric/main.go            process entrypoint: load config, run internal/app
├── config/shards.example.yaml      documented example config (every option shown with its default)
└── internal/
    ├── app/                        wires config -> shard map, router, pools, health checker, listener
    ├── config/                     YAML loading + validation; defaults for the optional blocks
    ├── shardmap/                   routing table: shard -> {primary, replicas, status, lag}
    ├── router/                     consistent-hash ring; consistency- and health-aware node choice
    ├── pool/                       one pgx pool per backend node; Drain / DrainAsync / Close
    ├── health/                     heartbeats, replication lag, failover controller, split-brain guard
    ├── proxy/                      Postgres wire protocol (simple query), routing hints, result streaming
    ├── metrics/                    placeholder: Prometheus export is not implemented yet
    └── chaos/                      failure-injection harness against a real cluster (build tag `chaos`)
```

### Component responsibilities

| Component | Responsibility |
|---|---|
| Listener / Session Manager | Accepts client TCP connections, one goroutine per connection, speaks Postgres wire protocol to the client |
| Router | Extracts shard key from the query/session, consistent-hash lookup into the shard map, decides primary vs. replica based on requested consistency level |
| Connection Pool Manager | Maintains a pool per backend node; drains a pool gracefully on node failure; caps total connections (bulkhead) so one bad node can't starve the rest |
| Shard Map | The routing table: `shard_key_range → {primary, [replicas], replica_lag}`. In-memory for v1, promotable to etcd/Consul later for proxy-level HA |
| Health Checker / Failover Controller | Background sweeps heartbeat every node in parallel and track `UP/SUSPECT/DOWN` + replication lag; while a primary is `DOWN`, drives failover: split-brain guard (replicas as witnesses), `pg_promote()`, then an atomic shard-map swap |
| Metrics / Observability | *Planned, not implemented:* pool utilization, replication lag per replica, failover events, query latency as Prometheus metrics |

This mirrors the build order below: start with a static shard map and no failover, then layer in replicas/read-write splitting, then health checks + failover, then consistency-level routing, then chaos test.

## Data flow

### Write path

1. Client sends a write query to the proxy over its persistent connection.
2. Listener hands the raw query to the Router.
3. Router extracts the shard key (e.g., from a bind parameter, session tag, or parsed `WHERE` clause) and hashes it to find the owning shard.
4. Router looks up that shard's **primary** in the Shard Map — writes always go to primary, never a replica.
5. Pool Manager checks out a connection to that primary (or opens one if the pool has headroom).
6. Query is forwarded, proxy waits for the backend's response.
7. Response streamed back to the client; connection returned to the pool.

### Read path

1. Client sends a read query, optionally tagged with a consistency requirement (session-level default, or a per-query hint).
2. Router resolves the shard as in the write path.
3. Consistency decision:
   - **strong** → route to primary (same as a write).
   - **eventual** → route to any replica in the shard's replica set (round-robin or least-connections).
   - **bounded-staleness(X ms)** → check each replica's last-known lag (updated by the Health Checker); route to a replica with `lag < X`, else fall back to primary.
4. Pool Manager checks out the chosen node's connection, forwards, streams response back.

### Health-check / lag-tracking flow (background, decoupled from request path)

1. Every `health_check_interval` a sweep checks every shard **in parallel**. Within a shard the primary and each replica are also pinged in parallel, each with a timeout of half the interval so one wedged node can't stall the sweep: `SELECT 1` for the primary, a replication-lag query for replicas. A sweep also runs once immediately at startup, so status and lag are populated right away.
2. Consecutive misses are counted per node: `suspect_after_misses` → `SUSPECT`, `down_after_misses` → `DOWN`. Any successful ping resets the count. A replica whose ping fails keeps its last known lag rather than being reset to 0, which would make a dead replica look perfectly fresh.
3. At most one check runs per shard at a time — a shard whose previous check is still running (typically a slow failover) is skipped by later sweeps rather than overlapped, and doesn't delay other shards. That guard is also what makes it safe for a check to replace a shard's entry in the shard map: nothing else writes that shard concurrently.
4. The shard's goroutine writes back a **whole replacement `Shard`** — never mutating one that readers already hold. That table is what the Router reads for health- and lag-aware routing; a request never pings a node to decide where to go.

**How lag is measured.** A replica reports `0` when it has replayed everything it has received; otherwise the time since its last replayed transaction (`now() - pg_last_xact_replay_timestamp()`). The obvious formula — that timestamp difference alone — is wrong on an idle primary: it grows without bound even for a fully caught-up replica (measured: 4.5 s "lag" on an idle, healthy replica), which would exclude every replica from bounded-staleness reads. The trade-off is that this measures *replay* lag behind what the replica has received, not how far the *receive* side trails the primary.

### Failover flow

1. The Health Checker marks a primary `SUSPECT` after `suspect_after_misses` missed heartbeats and `DOWN` after `down_after_misses` consecutive ones (avoids flapping on a single blip).
2. **While the primary stays `DOWN`, every sweep attempts failover** — not just the first. A promotion that was vetoed, or had no healthy replica to promote, is retried as conditions change; a primary that starts answering again (a healed partition) cancels the whole thing. Repeated identical failures are logged once, not every sweep.
3. Each attempt (`Controller.Promote`) runs in this order, and the order is the point:
   1. **Pick the candidate:** the least-lagged replica that isn't `DOWN`.
   2. **Split-brain guard** (`quorum_guard`): the proxy failing to reach the primary only proves the *proxy* can't — the primary may be alive for everyone else. So ask each non-`DOWN` replica whether its WAL receiver is still streaming from the primary (`pg_stat_wal_receiver`). Promote only if a **strict majority of the replicas that answered** cannot see the primary; a tie, or no answers, vetoes. When unsure, don't create a second primary.
   3. **`pg_promote()`** (`pg_promote`) on the candidate, waiting for it to finish. If this fails, the shard map is left untouched: routing never moves to a node that can't take writes. Already being a primary (SQLSTATE `55000`) counts as success, so the step is idempotent.
   4. **Swap the shard map entry** in one `Set`: new primary in, old primary out, candidate removed from the replica list.
   5. **Drain the old primary's pool.** The pool is detached immediately, so nothing new is routed through it; closing it waits for in-flight queries, so that happens in the background rather than holding up the failover.
4. Queries that were in flight to the old primary fail and are surfaced to the client as errors. The proxy never retries them itself, because a write whose outcome is unknown could be applied twice.
5. New writes and strong reads go to the promoted node from the next `Resolve`; shard-map reads are never cached.
6. The failover is logged. (Exporting failover count/timing as metrics is planned but not built.)

## Suggested build order

1. Static shard map, no failover — just route by hash → get sharding right first ✅
2. Add replicas + read/write splitting ✅
3. Add health checks + automatic failover ✅
4. Add consistency-level routing ✅
5. Chaos test: kill a primary mid-write-burst, kill a replica, partition the proxy from a node — measure recovery time and any lost/duplicated writes ✅

Each failure mode in step 5 is independently injectable against the flows above:
- Kill a primary mid-write-burst → exercises the failover flow end-to-end, measure writes lost/duplicated and time-to-recovery.
- Kill a replica → exercises the health-check flow (should just drop out of the eventual/bounded read pool, no failover needed).
- Partition the proxy from a node (not kill it) → exercises the split-brain guard specifically, since the node is alive for clients but not for the proxy.

### Chaos harness

`internal/chaos` builds a **real** two-node PostgreSQL cluster (`initdb` + `pg_basebackup` streaming replication), runs the proxy in-process in front of it, drives four concurrent at-least-once writers through the proxy (each retries an insert until it is acknowledged, which is what makes lost and duplicated writes observable), injects the failure, and measures. A partition is modelled by a TCP relay between the proxy and a *live* primary: cutting it makes the primary unreachable to the proxy while it stays up and keeps feeding its replica — the situation the split-brain guard exists for, and one that killing the primary can't produce.

```
make chaos        # or: go test -tags chaos -v -timeout 15m ./internal/chaos
```

It needs PostgreSQL server binaries (`PG_BIN`, PATH, or the default Windows install dir; the tests skip if none is found) and is kept out of `go test ./...`, which needs no database at all.

Measured on one Windows machine over localhost, PostgreSQL 18, health sweep every 500 ms and `DOWN` after 3 misses (so ~1.5 s is the floor for detection):

| Scenario | Result |
|---|---|
| Kill primary mid-write-burst (4 runs) | Writes stalled **1.5–2.0 s** end to end (detection + `pg_promote` + reroute). **0** acknowledged writes lost across ~7,400 acked before failover, **0** duplicates. ~100 client-visible errors during the stall (never silently retried by the proxy). |
| Kill a replica | Replica marked `DOWN` ~1.1–1.3 s after the kill. **0** write errors. ~25 eventual-consistency reads failed in that detection window (they were still routed to the dead replica; the proxy doesn't retry reads elsewhere) and **0** after. No failover triggered. |
| Partition from a *live* primary, guard **on** (3 runs) | Every promotion attempt vetoed (`1 replica(s) still see primary, 0 cannot`). No promotion, replica stayed read-only; writes were unavailable for the ~5.6 s partition (~410 client errors) and resumed after the heal. **0** acked writes lost, **0** duplicates. |
| Same partition, guard **off** | The proxy promotes the replica while the old primary is still alive: **two writable primaries**, data diverged (a row written directly to the old primary exists only there; a row written through the proxy exists only on the promoted node). Asserted in the harness so it can't silently stop being true. |

Caveats on reading these numbers: localhost replication lag is sub-millisecond, so loss to *asynchronous* replication at the moment of a crash is possible in principle but unlikely to show up here; a production network would widen that window. Timings vary run to run.

The harness paid for itself: it found two real proxy bugs that the hermetic suite had missed — a premature `CommandComplete` for column-less statements (writes acknowledged before they had completed) and an incomplete startup handshake that made pgx refuse to run queries. Both are fixed and covered by regression tests; details in `work.md`.

## Using it

```
go build -o bin/dbfabric ./cmd/dbfabric
./bin/dbfabric -config config/shards.example.yaml
```

### Routing simulator UI

Run the database-free control room against the same shard configuration:

```
go run ./cmd/dbfabric -config config/shards.example.yaml -ui 127.0.0.1:8080
```

Open `http://127.0.0.1:8080`. The UI exercises the real consistent-hash
router and lets you resolve strong, eventual, and bounded-staleness reads;
toggle node failures; inject replica lag; and promote the least-lagged
healthy replica. The `-ui` mode does not connect to PostgreSQL or start the
proxy listener.

Every option is documented in [config/shards.example.yaml](config/shards.example.yaml), shown with its default: `health.suspect_after_misses`, `health.down_after_misses`, `health.pg_promote`, `health.quorum_guard`, `routing.default_max_lag_ms`.

Clients connect with a normal Postgres client, subject to the constraints below. Because general SQL doesn't carry a shard key, routing uses two **interim conventions** (placeholders for a real hint mechanism, not SQL parsing):

- **Shard key:** the connection's `application_name` (falling back to `user`).
- **Consistency:** an optional *leading* SQL comment — `-- consistency=strong` (the default), `-- consistency=eventual`, or `-- consistency=bounded_staleness[:<max lag ms>]` (the ceiling defaults to `routing.default_max_lag_ms`). Case-insensitive; a comment that isn't first is ignored.

```
psql "host=127.0.0.1 port=5433 user=app application_name=user-42 sslmode=disable" \
     -c "-- consistency=bounded_staleness:500
         SELECT count(*) FROM orders"
```

Constraints on what can connect and what it can run — see the limitations below for the reasoning:

- The **simple query protocol** only. `psql` works; pgx needs `QueryExecModeSimpleProtocol`; drivers that default to the extended protocol (Parse/Bind/Execute) are not supported yet.
- **One statement per query message.** A message containing several statements (`SELECT 1; SELECT 2`) is rejected by the backend.
- **No transactions, and no dependable session state.** Each query runs on whichever pooled backend connection is free. `BEGIN`, `START TRANSACTION`, `COMMIT`, `END`, `ROLLBACK`, `ABORT`, `SAVEPOINT` and `RELEASE` are **refused with an explanatory error** rather than forwarded — forwarding them was measured to be worse than failing (see below). `SET`, temp tables and session-level prepared statements only "work" when the pool happens to hand back the same connection, so don't rely on them.
- The backend role must be allowed to call `pg_promote()` (superuser or an explicit `EXECUTE` grant) and to read `pg_stat_wal_receiver`'s status (superuser or `pg_monitor`), if failover with the guard is enabled.

