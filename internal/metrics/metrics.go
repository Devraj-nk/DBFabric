// Package metrics exports the proxy's operational state — pool
// utilization, replication lag per replica, failover events, query
// latency — as Prometheus metrics.
package metrics

// TODO: wire up prometheus/client_golang counters/gauges/histograms
// once that dependency is added, e.g.:
//   - pool_connections_in_use{node}
//   - replica_lag_ms{node}
//   - failover_total{shard}
//   - query_duration_seconds{shard,consistency}
