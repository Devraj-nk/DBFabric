// Package proxy is the data-plane entry point: it accepts client
// connections and, per connection, speaks the Postgres wire protocol
// to the client while the router/pool decide which backend handles
// each query.
package proxy

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strings"

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

// handleConn drives one client connection end to end: the startup
// handshake, then a loop of simple-query messages until the client
// terminates or a read/write fails.
func (l *Listener) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	params, err := readStartupParams(rw)
	if err != nil {
		log.Printf("proxy: startup handshake with %s failed: %v", conn.RemoteAddr(), err)
		return
	}
	shardKey := shardKeyFromParams(params)

	if err := writeAuthenticationOK(rw); err != nil {
		return
	}
	if err := writeParameterStatus(rw, "server_version", "14.0 (dbfabric)"); err != nil {
		return
	}
	if err := writeParameterStatus(rw, "client_encoding", "UTF8"); err != nil {
		return
	}
	if err := writeBackendKeyData(rw, 0, 0); err != nil {
		return
	}
	if err := writeReadyForQuery(rw, 'I'); err != nil {
		return
	}
	if err := rw.Flush(); err != nil {
		return
	}

	for {
		msgType, payload, err := readMessage(rw.Reader)
		if err != nil {
			if err != io.EOF {
				log.Printf("proxy: reading message from %s: %v", conn.RemoteAddr(), err)
			}
			return
		}

		switch msgType {
		case 'Q':
			query := strings.TrimRight(string(payload), "\x00")
			if err := handleQuery(ctx, rw, l.router, l.pools, shardKey, query); err != nil {
				return
			}
		case 'X':
			return
		default:
			if err := writeErrorResponse(rw, "ERROR", "0A000", fmt.Sprintf("unsupported message type %q", msgType)); err != nil {
				return
			}
		}

		// The simple query protocol requires a ReadyForQuery after every
		// command (success or error) — without it a real client (psql
		// included) blocks forever waiting for the backend to say it can
		// accept the next command.
		if err := writeReadyForQuery(rw, 'I'); err != nil {
			return
		}
		if err := rw.Flush(); err != nil {
			return
		}
	}
}
