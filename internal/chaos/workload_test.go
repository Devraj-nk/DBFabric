//go:build chaos

package chaos

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// dialProxy opens a client connection to the proxy the way an application
// would. The proxy speaks only the simple query protocol, so pgx is told
// not to use the extended one; application_name is the shard-key carrier.
func dialProxy(ctx context.Context, proxyAddr, appName string) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(fmt.Sprintf(
		"postgres://app@%s/postgres?sslmode=disable&connect_timeout=2&application_name=%s", proxyAddr, appName))
	if err != nil {
		return nil, err
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	return pgx.ConnectConfig(ctx, cfg)
}

// workload is a set of concurrent writers with at-least-once semantics: each
// inserts a request id, and if the insert errors (so its outcome is unknown)
// retries the same id until it succeeds. That is how a naive client behaves,
// and it is what makes lost and duplicated writes observable afterwards.
type workload struct {
	proxyAddr string
	nextID    atomic.Int64
	cancel    context.CancelFunc
	wg        sync.WaitGroup

	mu       sync.Mutex
	acked    map[int64]time.Time // req_id -> when the client saw the ack
	attempts int
	errTimes []time.Time
}

func startWorkload(t *testing.T, proxyAddr string, workers int) *workload {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	w := &workload{proxyAddr: proxyAddr, cancel: cancel, acked: map[int64]time.Time{}}
	for i := 0; i < workers; i++ {
		w.wg.Add(1)
		go w.worker(ctx)
	}
	t.Cleanup(w.stop)
	return w
}

// stop ends the workload after each writer's in-flight operation resolves.
// Safe to call more than once.
func (w *workload) stop() {
	w.cancel()
	w.wg.Wait()
}

func (w *workload) worker(ctx context.Context) {
	defer w.wg.Done()
	var conn *pgx.Conn
	defer func() {
		if conn != nil {
			conn.Close(context.Background())
		}
	}()

	for ctx.Err() == nil {
		id := w.nextID.Add(1)
		for ctx.Err() == nil {
			if conn == nil {
				dctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				c, err := dialProxy(dctx, w.proxyAddr, "user-1")
				cancel()
				if err != nil {
					w.recordError()
					time.Sleep(50 * time.Millisecond)
					continue
				}
				conn = c
			}

			w.mu.Lock()
			w.attempts++
			w.mu.Unlock()

			octx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_, err := conn.Exec(octx, fmt.Sprintf("INSERT INTO writes(req_id) VALUES (%d)", id))
			cancel()
			if err == nil {
				w.mu.Lock()
				w.acked[id] = time.Now()
				w.mu.Unlock()
				break
			}
			w.recordError()
			conn.Close(context.Background())
			conn = nil
			time.Sleep(50 * time.Millisecond)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func (w *workload) recordError() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.errTimes = append(w.errTimes, time.Now())
}

func (w *workload) ackCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.acked)
}

func (w *workload) errorCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.errTimes)
}

func (w *workload) attemptCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.attempts
}

// firstAckAfter returns the earliest acknowledgement at or after t0.
func (w *workload) firstAckAfter(t0 time.Time) (time.Time, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var first time.Time
	found := false
	for _, at := range w.acked {
		if !at.Before(t0) && (!found || at.Before(first)) {
			first, found = at, true
		}
	}
	return first, found
}

// longestGap returns the longest interval between two consecutive
// acknowledged writes (across all writers) and when it began. That is the
// write stall a client experienced, and unlike "time since the kill command
// returned" it doesn't depend on when the server actually stopped answering.
func (w *workload) longestGap() (gap time.Duration, began time.Time) {
	w.mu.Lock()
	times := make([]time.Time, 0, len(w.acked))
	for _, at := range w.acked {
		times = append(times, at)
	}
	w.mu.Unlock()

	sort.Slice(times, func(i, j int) bool { return times[i].Before(times[j]) })
	for i := 1; i < len(times); i++ {
		if d := times[i].Sub(times[i-1]); d > gap {
			gap, began = d, times[i-1]
		}
	}
	return gap, began
}

// ackedSplit returns the acknowledged ids acked before t0 and at/after it.
func (w *workload) ackedSplit(t0 time.Time) (before, after []int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for id, at := range w.acked {
		if at.Before(t0) {
			before = append(before, id)
		} else {
			after = append(after, id)
		}
	}
	return before, after
}

// readLoop repeatedly issues eventual-consistency reads through the proxy
// and records when they fail.
type readLoop struct {
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu       sync.Mutex
	errTimes []time.Time
	firstErr error
	ok       int
}

func startReadLoop(t *testing.T, proxyAddr string) *readLoop {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := &readLoop{cancel: cancel}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		var conn *pgx.Conn
		defer func() {
			if conn != nil {
				conn.Close(context.Background())
			}
		}()
		for ctx.Err() == nil {
			if conn == nil {
				dctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				c, err := dialProxy(dctx, proxyAddr, "reader-1")
				cancel()
				if err != nil {
					r.recordError(err)
					time.Sleep(50 * time.Millisecond)
					continue
				}
				conn = c
			}
			octx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			var n string
			err := conn.QueryRow(octx, "-- consistency=eventual\nSELECT count(*) FROM writes").Scan(&n)
			cancel()
			if err != nil {
				r.recordError(err)
				conn.Close(context.Background())
				conn = nil
				time.Sleep(50 * time.Millisecond)
				continue
			}
			r.mu.Lock()
			r.ok++
			r.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}()
	t.Cleanup(r.stop)
	return r
}

func (r *readLoop) stop() {
	r.cancel()
	r.wg.Wait()
}

func (r *readLoop) recordError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errTimes = append(r.errTimes, time.Now())
	if r.firstErr == nil {
		r.firstErr = err
	}
}

func (r *readLoop) firstError() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.firstErr
}

// errorsAfter counts read failures at or after t0.
func (r *readLoop) errorsAfter(t0 time.Time) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, at := range r.errTimes {
		if !at.Before(t0) {
			n++
		}
	}
	return n
}

func (r *readLoop) totals() (ok, errs int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ok, len(r.errTimes)
}
