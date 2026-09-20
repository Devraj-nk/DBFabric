package proxy

import (
	"bufio"
	"bytes"
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"dbfabric/internal/pool"
	"dbfabric/internal/router"
	"dbfabric/internal/shardmap"
)

// freeAddr returns a TCP address that is guaranteed to be refusing
// connections: it briefly listens on an OS-assigned port, then closes
// it, so nothing is bound there when the test runs a query against it.
// Used to test the proxy's error path without needing a real Postgres
// backend.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func newTestListener(t *testing.T) (l *Listener, primaryAddr, replicaAddr string) {
	t.Helper()
	primaryAddr = freeAddr(t)
	replicaAddr = freeAddr(t)

	sm := shardmap.New()
	rt := router.New(sm)
	rt.AddShard(&shardmap.Shard{
		ID:       "shard-0",
		Primary:  shardmap.Node{Addr: primaryAddr},
		Replicas: []shardmap.Node{{Addr: replicaAddr}},
	})
	pm := pool.NewManager(pool.Backend{User: "app", Database: "appdb"})
	return NewListener(":0", rt, pm, 1000), primaryAddr, replicaAddr
}

// drainHandshake reads messages until (and including) ReadyForQuery,
// mirroring what a real client's connection setup does.
func drainHandshake(t *testing.T, r *bufio.Reader) {
	t.Helper()
	for {
		msgType, _, err := readMessage(r)
		if err != nil {
			t.Fatalf("reading handshake message: %v", err)
		}
		if msgType == 'Z' {
			return
		}
	}
}

// TestHandleConn_HandshakeAnnouncesParametersDriversNeed is a regression
// test found by the chaos harness: pgx in simple-protocol mode refuses to
// run any Query unless it has seen standard_conforming_strings=on, and the
// handshake used to announce only server_version and client_encoding.
func TestHandleConn_HandshakeAnnouncesParametersDriversNeed(t *testing.T) {
	l, _, _ := newTestListener(t)
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	done := make(chan struct{})
	go func() {
		l.handleConn(context.Background(), serverConn)
		close(done)
	}()

	client := bufio.NewReadWriter(bufio.NewReader(clientConn), bufio.NewWriter(clientConn))
	startup := buildStartupMessage(map[string]string{"user": "app", "application_name": "user-42"})
	if _, err := client.Write(startup); err != nil {
		t.Fatal(err)
	}
	if err := client.Flush(); err != nil {
		t.Fatal(err)
	}

	got := map[string]string{}
	for {
		msgType, payload, err := readMessage(client.Reader)
		if err != nil {
			t.Fatalf("reading handshake message: %v", err)
		}
		if msgType == 'S' {
			// ParameterStatus is exactly "name\0value\0" (no extra
			// terminator, unlike a startup message's parameter list).
			kv := strings.Split(strings.TrimSuffix(string(payload), "\x00"), "\x00")
			if len(kv) != 2 {
				t.Fatalf("malformed ParameterStatus %q", payload)
			}
			got[kv[0]] = kv[1]
		}
		if msgType == 'Z' {
			break
		}
	}

	want := map[string]string{
		"standard_conforming_strings": "on",
		"client_encoding":             "UTF8",
		"server_encoding":             "UTF8",
		"integer_datetimes":           "on",
		"session_authorization":       "app",
		"application_name":            "user-42",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("ParameterStatus %q = %q, want %q (all announced: %v)", k, got[k], v, got)
		}
	}

	terminate(t, client)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleConn did not exit after Terminate")
	}
}

// Transaction control must be refused, not forwarded: forwarding it would
// let a client believe a ROLLBACK had undone writes that were in fact
// committed (see isTransactionControl). The refusal happens before routing,
// so it needs no backend, and the connection must stay usable afterward.
func TestHandleConn_TransactionControlIsRefusedAndConnectionSurvives(t *testing.T) {
	l, _, _ := newTestListener(t)
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	done := make(chan struct{})
	go func() {
		l.handleConn(context.Background(), serverConn)
		close(done)
	}()

	client := bufio.NewReadWriter(bufio.NewReader(clientConn), bufio.NewWriter(clientConn))
	startup := buildStartupMessage(map[string]string{"user": "app", "application_name": "user-1"})
	client.Write(startup)
	client.Flush()
	drainHandshake(t, client.Reader)

	for _, stmt := range []string{"BEGIN", "-- consistency=eventual\ncommit", "ROLLBACK"} {
		sendQuery(t, client, stmt)

		msgType, payload, err := readMessage(client.Reader)
		if err != nil || msgType != 'E' {
			t.Fatalf("%q: expected ErrorResponse, got %q, err=%v", stmt, msgType, err)
		}
		if msg := decodeErrorMessage(t, payload); !strings.Contains(msg, "not supported") {
			t.Errorf("%q: error message %q should explain that transactions are unsupported", stmt, msg)
		}
		msgType, _, err = readMessage(client.Reader)
		if err != nil || msgType != 'Z' {
			t.Fatalf("%q: expected ReadyForQuery after the refusal, got %q, err=%v", stmt, msgType, err)
		}
	}

	terminate(t, client)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleConn did not exit after Terminate")
	}
}

// sendQuery writes a simple-protocol Query message for query.
func sendQuery(t *testing.T, client *bufio.ReadWriter, query string) {
	t.Helper()
	var buf bytes.Buffer
	if err := writeMessage(&buf, 'Q', cString(query)); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Write(buf.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := client.Flush(); err != nil {
		t.Fatal(err)
	}
}

func terminate(t *testing.T, client *bufio.ReadWriter) {
	t.Helper()
	var buf bytes.Buffer
	if err := writeMessage(&buf, 'X', nil); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Write(buf.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := client.Flush(); err != nil {
		t.Fatal(err)
	}
}

// decodeErrorMessage extracts the human-readable message field ('M')
// from an ErrorResponse payload.
func decodeErrorMessage(t *testing.T, payload []byte) string {
	t.Helper()
	// ErrorResponse's payload is [1-byte field code][C-string]... ending
	// in a lone extra 0 (see writeErrorResponse) — the same "run of
	// null-terminated strings, one more 0 to end the run" shape
	// splitCStrings expects, just with each string's own field-code
	// byte glued onto its front instead of a separate token.
	fields, err := splitCStrings(payload)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range fields {
		if strings.HasPrefix(f, "M") {
			return f[1:]
		}
	}
	t.Fatalf("no message field ('M') found in ErrorResponse payload %q", payload)
	return ""
}

// runQueryExpectingError drives one handshake+query+terminate cycle
// against a fresh connection and returns the ErrorResponse message
// text — used to confirm both that a query failed and, by checking
// which address appears in the message, that it was routed correctly
// before the (inevitable, since nothing is listening) connection
// failure.
func runQueryExpectingError(t *testing.T, l *Listener, appName, query string) string {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	done := make(chan struct{})
	go func() {
		l.handleConn(context.Background(), serverConn)
		close(done)
	}()

	client := bufio.NewReadWriter(bufio.NewReader(clientConn), bufio.NewWriter(clientConn))

	startup := buildStartupMessage(map[string]string{"user": "app", "application_name": appName})
	if _, err := client.Write(startup); err != nil {
		t.Fatal(err)
	}
	if err := client.Flush(); err != nil {
		t.Fatal(err)
	}
	drainHandshake(t, client.Reader)

	sendQuery(t, client, query)

	msgType, payload, err := readMessage(client.Reader)
	if err != nil || msgType != 'E' {
		t.Fatalf("expected ErrorResponse ('E'), got %q, err=%v", msgType, err)
	}
	msg := decodeErrorMessage(t, payload)

	msgType, _, err = readMessage(client.Reader)
	if err != nil || msgType != 'Z' {
		t.Fatalf("expected ReadyForQuery ('Z') after the error, got %q, err=%v", msgType, err)
	}

	terminate(t, client)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleConn did not exit after Terminate")
	}

	return msg
}

func TestHandleConn_RoutesEventualToReplicaBeforeFailingToConnect(t *testing.T) {
	l, _, replicaAddr := newTestListener(t)

	// Only one shard is registered, so every key maps to it; Eventual
	// picks its (only) replica. The backend connection then fails
	// because nothing is listening on that address — but the error
	// message names the address it tried, proving routing chose the
	// replica, not the primary.
	msg := runQueryExpectingError(t, l, "user-42", "-- consistency=eventual\nSELECT 1")
	if !strings.Contains(msg, replicaAddr) {
		t.Errorf("error message %q does not mention the replica address %q — routing may be wrong", msg, replicaAddr)
	}
}

func TestHandleConn_DefaultsToStrongConsistencyRoutesToPrimary(t *testing.T) {
	l, primaryAddr, _ := newTestListener(t)

	msg := runQueryExpectingError(t, l, "user-42", "SELECT 1") // no consistency hint
	if !strings.Contains(msg, primaryAddr) {
		t.Errorf("error message %q does not mention the primary address %q — routing may be wrong", msg, primaryAddr)
	}
}

func TestHandleConn_ConnectionStaysUsableAfterAQueryError(t *testing.T) {
	l, primaryAddr, _ := newTestListener(t)
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	done := make(chan struct{})
	go func() {
		l.handleConn(context.Background(), serverConn)
		close(done)
	}()

	client := bufio.NewReadWriter(bufio.NewReader(clientConn), bufio.NewWriter(clientConn))
	startup := buildStartupMessage(map[string]string{"user": "app", "application_name": "user-1"})
	client.Write(startup)
	client.Flush()
	drainHandshake(t, client.Reader)

	// First query fails (no real backend); the connection must still
	// accept a second query afterward rather than being left unusable.
	for i := 0; i < 2; i++ {
		sendQuery(t, client, "SELECT 1")
		msgType, payload, err := readMessage(client.Reader)
		if err != nil || msgType != 'E' {
			t.Fatalf("query %d: expected ErrorResponse, got %q, err=%v", i, msgType, err)
		}
		if !strings.Contains(decodeErrorMessage(t, payload), primaryAddr) {
			t.Errorf("query %d: error message did not mention %q", i, primaryAddr)
		}
		msgType, _, err = readMessage(client.Reader)
		if err != nil || msgType != 'Z' {
			t.Fatalf("query %d: expected ReadyForQuery, got %q, err=%v", i, msgType, err)
		}
	}

	terminate(t, client)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleConn did not exit after Terminate")
	}
}
