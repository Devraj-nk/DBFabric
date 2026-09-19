package proxy

import (
	"bufio"
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"dbfabric/internal/pool"
	"dbfabric/internal/router"
	"dbfabric/internal/shardmap"
)

func newTestListener() *Listener {
	sm := shardmap.New()
	rt := router.New(sm)
	rt.AddShard(&shardmap.Shard{
		ID:       "shard-0",
		Primary:  shardmap.Node{Addr: "primary-0"},
		Replicas: []shardmap.Node{{Addr: "replica-0a"}},
	})
	return NewListener(":0", rt, pool.NewManager())
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

func TestHandleConn_HandshakeAndQueryRouting(t *testing.T) {
	l := newTestListener()
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	done := make(chan struct{})
	go func() {
		l.handleConn(context.Background(), serverConn)
		close(done)
	}()

	client := bufio.NewReadWriter(bufio.NewReader(clientConn), bufio.NewWriter(clientConn))

	startup := buildStartupMessage(map[string]string{
		"user":             "app",
		"application_name": "user-42",
	})
	if _, err := client.Write(startup); err != nil {
		t.Fatal(err)
	}
	if err := client.Flush(); err != nil {
		t.Fatal(err)
	}

	drainHandshake(t, client.Reader)

	var qbuf bytes.Buffer
	if err := writeMessage(&qbuf, 'Q', cString("-- consistency=eventual\nSELECT 1")); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Write(qbuf.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := client.Flush(); err != nil {
		t.Fatal(err)
	}

	msgType, _, err := readMessage(client.Reader)
	if err != nil || msgType != 'T' {
		t.Fatalf("expected RowDescription ('T'), got %q, err=%v", msgType, err)
	}

	msgType, payload, err := readMessage(client.Reader)
	if err != nil || msgType != 'D' {
		t.Fatalf("expected DataRow ('D'), got %q, err=%v", msgType, err)
	}
	values := decodeDataRow(t, payload)
	if len(values) != 3 {
		t.Fatalf("expected 3 columns, got %d: %v", len(values), values)
	}
	if values[0] != "user-42" {
		t.Errorf("shard_key = %q, want %q", values[0], "user-42")
	}
	if values[1] != string(router.Eventual) {
		t.Errorf("consistency = %q, want %q", values[1], router.Eventual)
	}
	if values[2] != "replica-0a" {
		t.Errorf("routed_to = %q, want %q", values[2], "replica-0a")
	}

	msgType, _, err = readMessage(client.Reader)
	if err != nil || msgType != 'C' {
		t.Fatalf("expected CommandComplete ('C'), got %q, err=%v", msgType, err)
	}

	// A real client (psql included) blocks waiting for this before it'll
	// consider the query done — this is the follow-up ReadyForQuery.
	msgType, _, err = readMessage(client.Reader)
	if err != nil || msgType != 'Z' {
		t.Fatalf("expected trailing ReadyForQuery ('Z'), got %q, err=%v", msgType, err)
	}

	var termBuf bytes.Buffer
	if err := writeMessage(&termBuf, 'X', nil); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Write(termBuf.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := client.Flush(); err != nil {
		t.Fatal(err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleConn did not exit after Terminate")
	}
}

func TestHandleConn_DefaultsToStrongConsistency(t *testing.T) {
	l := newTestListener()
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()

	done := make(chan struct{})
	go func() {
		l.handleConn(context.Background(), serverConn)
		close(done)
	}()

	client := bufio.NewReadWriter(bufio.NewReader(clientConn), bufio.NewWriter(clientConn))

	startup := buildStartupMessage(map[string]string{"user": "app", "application_name": "user-42"})
	client.Write(startup)
	client.Flush()
	drainHandshake(t, client.Reader)

	var qbuf bytes.Buffer
	writeMessage(&qbuf, 'Q', cString("SELECT 1")) // no consistency hint
	client.Write(qbuf.Bytes())
	client.Flush()

	readMessage(client.Reader) // RowDescription
	_, payload, err := readMessage(client.Reader)
	if err != nil {
		t.Fatal(err)
	}
	values := decodeDataRow(t, payload)
	if values[1] != string(router.Strong) {
		t.Errorf("consistency = %q, want %q (default)", values[1], router.Strong)
	}
	if values[2] != "primary-0" {
		t.Errorf("routed_to = %q, want primary-0", values[2])
	}
	readMessage(client.Reader) // CommandComplete
	readMessage(client.Reader) // ReadyForQuery

	var termBuf bytes.Buffer
	writeMessage(&termBuf, 'X', nil)
	client.Write(termBuf.Bytes())
	client.Flush()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleConn did not exit after Terminate")
	}
}
