// Package pool manages one pgx connection pool per backend node, so a
// failed or slow node can be drained without leaking connections or
// starving the rest of the pool (bulkhead isolation).
package pool

import (
	"context"
	"fmt"
	"net/url"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Backend holds the credentials used to connect to every backend
// node. The proxy assumes uniform credentials across the shard fleet
// (one application role, per-shard databases addressed by host:port)
// — typical for a sharded deployment running the same schema/role
// everywhere; per-shard credentials would need a richer shard-map
// entry and aren't needed yet.
type Backend struct {
	User     string
	Password string
	Database string
	SSLMode  string // e.g. "disable", "require"; defaults to "disable"
}

func (b Backend) dsn(addr string) string {
	sslmode := b.SSLMode
	if sslmode == "" {
		sslmode = "disable"
	}
	u := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(b.User, b.Password),
		Host:   addr,
		Path:   "/" + b.Database,
	}
	q := u.Query()
	q.Set("sslmode", sslmode)
	u.RawQuery = q.Encode()
	return u.String()
}

// Manager owns one *pgxpool.Pool per backend node address, created
// lazily on first use. pgxpool.New itself doesn't dial eagerly, so
// Get is cheap even for a node that turns out to be unreachable — the
// actual connection attempt happens on the first query against it.
type Manager struct {
	backend Backend

	mu    sync.Mutex
	pools map[string]*pgxpool.Pool
}

func NewManager(backend Backend) *Manager {
	return &Manager{backend: backend, pools: make(map[string]*pgxpool.Pool)}
}

// Get returns the pool for addr, creating it if this is the first
// request for that node.
func (m *Manager) Get(ctx context.Context, addr string) (*pgxpool.Pool, error) {
	m.mu.Lock()
	if p, ok := m.pools[addr]; ok {
		m.mu.Unlock()
		return p, nil
	}
	m.mu.Unlock()

	p, err := pgxpool.New(ctx, m.backend.dsn(addr))
	if err != nil {
		return nil, fmt.Errorf("pool: configuring pool for %s: %w", addr, err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.pools[addr]; ok {
		p.Close() // another goroutine won the race to create this node's pool
		return existing, nil
	}
	m.pools[addr] = p
	return p, nil
}

// Drain closes and removes a node's pool, e.g. after the health
// checker marks it DOWN. In-flight queries against it should fail
// fast and be surfaced to the client rather than silently retried by
// the proxy, to avoid duplicate writes.
func (m *Manager) Drain(addr string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if p, ok := m.pools[addr]; ok {
		p.Close()
		delete(m.pools, addr)
	}
}
