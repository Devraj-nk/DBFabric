package health

import (
	"context"
	"errors"
	"sync"
	"time"
)

var errUnreachable = errors.New("fake: node unreachable")

// fakeBackends is a controllable stand-in for pgxBackends. Every
// operation is recorded in calls (in order) so tests can assert sequencing,
// e.g. that a quorum vote happens before pg_promote, which happens before
// the old primary's pool is drained.
type fakeBackends struct {
	mu sync.Mutex

	down       map[string]bool  // addr -> Ping/ReplicationLagMS fail
	lag        map[string]int64 // addr -> reported replication lag
	streaming  map[string]bool  // addr -> WAL receiver is streaming
	streamErr  map[string]error // addr -> WalReceiverStreaming fails
	promoteErr map[string]error // addr -> PgPromote fails

	calls []string

	// Optional hooks, invoked outside the lock so they may block.
	pingHook    func(addr string)
	promoteHook func(addr string)
}

func newFakeBackends() *fakeBackends {
	return &fakeBackends{
		down:       map[string]bool{},
		lag:        map[string]int64{},
		streaming:  map[string]bool{},
		streamErr:  map[string]error{},
		promoteErr: map[string]error{},
	}
}

func (f *fakeBackends) setDown(addr string, down bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.down[addr] = down
}

func (f *fakeBackends) setLag(addr string, lagMS int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lag[addr] = lagMS
}

func (f *fakeBackends) setStreaming(addr string, streaming bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.streaming[addr] = streaming
}

func (f *fakeBackends) setPromoteErr(addr string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.promoteErr[addr] = err
}

func (f *fakeBackends) record(call string) {
	f.calls = append(f.calls, call)
}

// callLog returns a copy of the recorded operations.
func (f *fakeBackends) callLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

// count returns how many recorded calls equal call.
func (f *fakeBackends) count(call string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if c == call {
			n++
		}
	}
	return n
}

func (f *fakeBackends) Ping(ctx context.Context, addr string) error {
	if f.pingHook != nil {
		f.pingHook(addr)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down[addr] {
		return errUnreachable
	}
	return nil
}

func (f *fakeBackends) ReplicationLagMS(ctx context.Context, addr string) (int64, error) {
	if f.pingHook != nil {
		f.pingHook(addr)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down[addr] {
		return 0, errUnreachable
	}
	return f.lag[addr], nil
}

func (f *fakeBackends) WalReceiverStreaming(ctx context.Context, addr string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("witness:" + addr)
	if err := f.streamErr[addr]; err != nil {
		return false, err
	}
	return f.streaming[addr], nil
}

func (f *fakeBackends) PgPromote(ctx context.Context, addr string, wait time.Duration) error {
	f.mu.Lock()
	f.record("promote:" + addr)
	hook := f.promoteHook
	f.mu.Unlock()
	if hook != nil {
		hook(addr)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.promoteErr[addr]
}

func (f *fakeBackends) Drain(addr string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.record("drain:" + addr)
}
