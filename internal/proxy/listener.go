// Package proxy is the data-plane entry point: it accepts client
// connections and, per connection, speaks the Postgres wire protocol
// to the client while the router/pool decide which backend handles
// each query.
package proxy

import (
	"context"
	"net"

	"dbfabric/internal/pool"
	"dbfabric/internal/router"
)

// Listener accepts client TCP connections, one goroutine per
// connection.
type Listener struct {
	addr   string
	router *router.Router
	pools  *pool.Manager
}

func NewListener(addr string, rt *router.Router, pm *pool.Manager) *Listener {
	return &Listener{addr: addr, router: rt, pools: pm}
}

// Run starts accepting connections and blocks until ctx is cancelled
// or the listener fails.
//
// TODO: per-connection goroutine that speaks the Postgres wire
// protocol, extracts the shard key/consistency hint per query, and
// forwards to the node the router resolves.
func (l *Listener) Run(ctx context.Context) error {
	lis, err := net.Listen("tcp", l.addr)
	if err != nil {
		return err
	}
	defer lis.Close()

	go func() {
		<-ctx.Done()
		lis.Close()
	}()

	for {
		conn, err := lis.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				return err
			}
		}
		go l.handleConn(ctx, conn)
	}
}

func (l *Listener) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	// TODO: wire protocol handshake, query loop, routing, forwarding.
}
