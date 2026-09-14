# DBFabric

Sharded, Replicated Database Proxy — Deep Dive

## Core idea

You sit a proxy between your application and a set of Postgres/MySQL instances. The app talks only to the proxy; the proxy decides which physical database handles each query.

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
                         ┌─────────────────────────────┐
                         │           Clients            │
                         │  (apps speaking Postgres wire│
                         │   protocol, unmodified)       │
                         └───────────────┬───────────────┘
                                         │
                                         ▼
                         ┌─────────────────────────────┐
                         │            PROXY              │
                         │                                │
                         │  ┌──────────────────────────┐  │
                         │  │  Listener / Session Mgr   │  │  data plane
                         │  │  (net.Listener,            │  │
                         │  │   one goroutine/conn)      │  │
                         │  └────────────┬─────────────┘  │
                         │               ▼                 │
                         │  ┌──────────────────────────┐  │
                         │  │        Router             │  │
                         │  │  - extract shard key       │  │
                         │  │  - consistent hash lookup  │  │
                         │  │  - consistency-level logic │  │
                         │  └────────────┬─────────────┘   │
                         │               ▼                  │
                         │  ┌────────────────────────────┐  │
                         │  │   Connection Pool Manager  │  │
                         │  │  (per-node pools, bulkhead │  │
                         │  │   isolation on failure)    │  │
                         │  └────────────┬───────────────┘  │
                         │               │                 │
                         │  ┌────────────┴─────────────┐  │
                         │  │      Shard Map (state)     │  │  control plane
                         │  │  shard_key_range → primary  │  │
                         │  │  + replica set + lag info  │  │
                         │  └────────────┬─────────────┘  │
                         │               ▲                 │
                         │  ┌────────────┴─────────────┐  │
                         │  │  Health Checker /          │  │
                         │  │  Failover Controller       │  │
                         │  │  (heartbeats, promotion,   │  │
                         │  │   split-brain guards)      │  │
                         │  └──────────────────────────┘  │
                         │                                │
                         │  ┌──────────────────────────┐  │
                         │  │  Metrics / Observability   │  │
                         │  │  (Prometheus exporter)     │  │
                         │  └──────────────────────────┘  │
                         └───────────────┬───────────────┘
                                         │
              ┌──────────────────────────┼──────────────────────────┐
              ▼                          ▼                          ▼
      ┌───────────────┐          ┌───────────────┐          ┌───────────────┐
      │   Shard 1      │          │   Shard 2      │          │   Shard N      │
      │ Primary + N    │          │ Primary + N    │          │ Primary + N    │
      │ Replicas       │          │ Replicas       │          │ Replicas       │
      │ (Postgres)     │          │ (Postgres)     │          │ (Postgres)     │
      └───────────────┘          └───────────────┘          └───────────────┘
```

### Component responsibilities

| Component | Responsibility |
|---|---|
| Listener / Session Manager | Accepts client TCP connections, one goroutine per connection, speaks Postgres wire protocol to the client |
| Router | Extracts shard key from the query/session, consistent-hash lookup into the shard map, decides primary vs. replica based on requested consistency level |
| Connection Pool Manager | Maintains a pool per backend node; drains a pool gracefully on node failure; caps total connections (bulkhead) so one bad node can't starve the rest |
| Shard Map | The routing table: `shard_key_range → {primary, [replicas], replica_lag}`. In-memory for v1, promotable to etcd/Consul later for proxy-level HA |
| Health Checker / Failover Controller | Background loop heartbeating every node; on primary failure, triggers promotion and atomically updates the shard map; guards against split-brain |
| Metrics / Observability | Exposes pool utilization, replication lag per replica, failover events, query latency as Prometheus metrics |

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

1. A goroutine per node pings its primary/replica on a ticker (e.g., lightweight `SELECT 1` + replication-lag query on replicas), each with a `context` timeout so one wedged node can't hang the checker.
2. Results are sent over a channel to a single goroutine that owns the node-status table (`UP / SUSPECT / DOWN` + last-seen lag) — one writer, no lock contention with the read path.
3. This table is what the Router's bounded-staleness logic reads — it never blocks a request to check lag live.

### Failover flow

1. Health Checker marks a primary `SUSPECT` after one missed heartbeat, `DOWN` only after N consecutive misses (avoids flapping on a single blip).
2. On `DOWN`, Failover Controller:
   - Triggers promotion — either an internal election among replicas, or a call out to an external orchestrator (Patroni-style) that already owns this job.
   - **Split-brain guard**: before promoting, confirm via a quorum check (e.g., can the majority of replicas also no longer reach the old primary?) rather than trusting the proxy's own network view alone — the proxy being partitioned from the primary doesn't mean the primary is actually down for clients.
   - Atomically swaps the shard map entry: new primary in, old primary demoted to "unreachable" (not silently reused even if it comes back, until it's confirmed to have rejoined as a replica).
3. Pool Manager drains the old primary's connection pool (in-flight queries fail fast and are surfaced to the client as retryable errors — not silently retried by the proxy, to avoid duplicate writes).
4. New writes/strong-reads are routed to the promoted node from this point on.
5. Event is logged and exported as a metric (failover count, detection time, promotion time) — this is the data the chaos test below is meant to produce.

## Suggested build order

1. Static shard map, no failover — just route by hash → get sharding right first
2. Add replicas + read/write splitting
3. Add health checks + automatic failover
4. Add consistency-level routing
5. Chaos test: kill a primary mid-write-burst, kill a replica, partition the proxy from a node — measure recovery time and any lost/duplicated writes

Each failure mode in step 5 is independently injectable against the flows above:
- Kill a primary mid-write-burst → exercises the failover flow end-to-end, measure writes lost/duplicated and time-to-recovery.
- Kill a replica → exercises the health-check flow (should just drop out of the eventual/bounded read pool, no failover needed).
- Partition the proxy from a node (not kill it) → exercises the split-brain guard specifically, since the node is alive for clients but not for the proxy.
