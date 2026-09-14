// Package pool manages one connection pool per backend node, so a
// failed or slow node can be drained without leaking connections or
// starving the rest of the pool (bulkhead isolation).
package pool

import "sync"

// Pool is a single backend node's connection pool.
//
// TODO: back this with *pgxpool.Pool once pgx is added as a dependency.
type Pool struct {
	Addr string
}

// Manager owns one Pool per backend node address.
type Manager struct {
	mu    sync.RWMutex
	pools map[string]*Pool
}

func NewManager() *Manager {
	return &Manager{pools: make(map[string]*Pool)}
}

// Get returns the pool for addr, creating it if it doesn't exist yet.
func (m *Manager) Get(addr string) *Pool {
	m.mu.RLock()
	p, ok := m.pools[addr]
	m.mu.RUnlock()
	if ok {
		return p
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if p, ok := m.pools[addr]; ok {
		return p
	}
	p = &Pool{Addr: addr}
	m.pools[addr] = p
	return p
}

// Drain removes a node's pool, e.g. after the health checker marks it
// DOWN. In-flight queries against it should fail fast rather than be
// silently retried by the proxy, to avoid duplicate writes.
func (m *Manager) Drain(addr string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.pools, addr)
}
