//go:build chaos

package chaos

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// forwarder is a TCP relay standing between the proxy and a real primary.
// Partition() makes it sever every connection and refuse new ones, so the
// proxy can no longer reach the primary while the primary itself stays up
// and stays reachable by everyone else — in particular by its replica,
// whose WAL receiver connects to the primary directly, not through here.
// That is precisely the "alive for clients, dead for the proxy" situation
// the split-brain guard exists for, and it can't be produced by killing
// the primary.
type forwarder struct {
	target string
	ln     net.Listener

	mu          sync.Mutex
	partitioned bool
	conns       map[net.Conn]struct{}
}

func newForwarder(t *testing.T, target string) *forwarder {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &forwarder{target: target, ln: ln, conns: map[net.Conn]struct{}{}}
	go f.acceptLoop()
	t.Cleanup(func() {
		ln.Close()
		f.closeAll()
	})
	return f
}

func (f *forwarder) addr() string { return f.ln.Addr().String() }

func (f *forwarder) acceptLoop() {
	for {
		client, err := f.ln.Accept()
		if err != nil {
			return
		}
		f.mu.Lock()
		refuse := f.partitioned
		f.mu.Unlock()
		if refuse {
			client.Close()
			continue
		}
		go f.relay(client)
	}
}

func (f *forwarder) relay(client net.Conn) {
	upstream, err := net.DialTimeout("tcp", f.target, 2*time.Second)
	if err != nil {
		client.Close()
		return
	}

	f.mu.Lock()
	if f.partitioned {
		f.mu.Unlock()
		client.Close()
		upstream.Close()
		return
	}
	f.conns[client] = struct{}{}
	f.conns[upstream] = struct{}{}
	f.mu.Unlock()

	var once sync.Once
	closeBoth := func() {
		once.Do(func() {
			client.Close()
			upstream.Close()
			f.mu.Lock()
			delete(f.conns, client)
			delete(f.conns, upstream)
			f.mu.Unlock()
		})
	}
	go func() { io.Copy(upstream, client); closeBoth() }()
	go func() { io.Copy(client, upstream); closeBoth() }()
}

// Partition severs every relayed connection and refuses new ones until Heal.
func (f *forwarder) Partition() {
	f.mu.Lock()
	f.partitioned = true
	f.mu.Unlock()
	f.closeAll()
}

// Heal lets connections through again.
func (f *forwarder) Heal() {
	f.mu.Lock()
	f.partitioned = false
	f.mu.Unlock()
}

func (f *forwarder) closeAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for c := range f.conns {
		c.Close()
	}
}
