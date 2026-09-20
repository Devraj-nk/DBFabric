//go:build chaos

package chaos

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// pgTools locates and runs the PostgreSQL server binaries. logDir is where
// each invocation's output file goes; it lives under the test's temp dir
// because a daemonized server keeps its inherited copy of that file open
// for as long as it runs, so on Windows the file can't be deleted right
// after the call — it has to wait for the test's own cleanup.
type pgTools struct {
	dir    string
	logDir string
	seq    *atomic.Int64
}

func findPGTools(t *testing.T) pgTools {
	t.Helper()
	has := func(dir string) bool {
		_, err := os.Stat(filepath.Join(dir, exeName("pg_ctl")))
		return err == nil
	}
	tools := func(dir string) pgTools {
		// t.TempDir is registered before any server exists, so it is
		// removed after the servers' own cleanups have stopped them.
		return pgTools{dir: dir, logDir: t.TempDir(), seq: new(atomic.Int64)}
	}

	if dir := os.Getenv("PG_BIN"); dir != "" {
		if !has(dir) {
			t.Fatalf("PG_BIN=%q does not contain pg_ctl", dir)
		}
		return tools(dir)
	}
	if p, err := exec.LookPath("pg_ctl"); err == nil {
		return tools(filepath.Dir(p))
	}
	if runtime.GOOS == "windows" {
		matches, _ := filepath.Glob(`C:\Program Files\PostgreSQL\*\bin`)
		best, bestVersion := "", -1
		for _, m := range matches {
			v, err := strconv.Atoi(filepath.Base(filepath.Dir(m)))
			if err == nil && v > bestVersion && has(m) {
				best, bestVersion = m, v
			}
		}
		if best != "" {
			return tools(best)
		}
	}
	t.Skip("PostgreSQL binaries not found: set PG_BIN to the directory containing pg_ctl, initdb and pg_basebackup")
	return pgTools{}
}

func exeName(name string) string {
	if runtime.GOOS == "windows" {
		return name + ".exe"
	}
	return name
}

// run executes one PostgreSQL tool and returns its combined output.
//
// Output goes to a file, never a pipe. `pg_ctl start` daemonizes the
// server, and the server inherits pg_ctl's stdout: with a pipe, reading it
// to EOF (which exec.Cmd.CombinedOutput does) blocks until the *server*
// exits, deadlocking every start.
func (p pgTools) run(name string, args ...string) (string, error) {
	out, err := os.Create(filepath.Join(p.logDir, fmt.Sprintf("%s-%d.log", name, p.seq.Add(1))))
	if err != nil {
		return "", err
	}

	cmd := exec.Command(filepath.Join(p.dir, exeName(name)), args...)
	cmd.Stdout, cmd.Stderr = out, out
	runErr := cmd.Run()
	out.Close()

	b, _ := os.ReadFile(out.Name())
	return string(b), runErr
}

func (p pgTools) mustRun(t *testing.T, name string, args ...string) {
	t.Helper()
	if out, err := p.run(name, args...); err != nil {
		t.Fatalf("%s %v failed: %v\n%s", name, args, err, out)
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// pgNode is one PostgreSQL server the harness controls.
type pgNode struct {
	t     *testing.T
	tools pgTools
	name  string
	dir   string
	port  int
}

func (n *pgNode) addr() string { return fmt.Sprintf("127.0.0.1:%d", n.port) }
func (n *pgNode) log() string  { return n.dir + ".log" }

func (n *pgNode) start() {
	n.t.Helper()
	n.tools.mustRun(n.t, "pg_ctl",
		"-D", n.dir,
		"-o", fmt.Sprintf("-p %d -h 127.0.0.1", n.port),
		"-l", n.log(), "-w", "start")
}

// kill stops the server the hard way (no shutdown checkpoint, like a crash
// of the postmaster's children) and returns once it is gone.
func (n *pgNode) kill() {
	n.t.Helper()
	n.tools.mustRun(n.t, "pg_ctl", "-D", n.dir, "-m", "immediate", "-w", "stop")
}

// stopQuietly is for cleanup: the node may already be dead.
func (n *pgNode) stopQuietly() {
	_, _ = n.tools.run("pg_ctl", "-D", n.dir, "-m", "immediate", "-w", "stop")
}

func (n *pgNode) connect(ctx context.Context) (*pgx.Conn, error) {
	return pgx.Connect(ctx, fmt.Sprintf("postgres://postgres@%s/postgres?sslmode=disable&connect_timeout=3", n.addr()))
}

func (n *pgNode) exec(sql string) {
	n.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := n.connect(ctx)
	if err != nil {
		n.t.Fatalf("%s: connect: %v", n.name, err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, sql); err != nil {
		n.t.Fatalf("%s: %s: %v", n.name, sql, err)
	}
}

// query1 runs a query returning a single text value.
func (n *pgNode) query1(sql string) string {
	n.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := n.connect(ctx)
	if err != nil {
		n.t.Fatalf("%s: connect: %v", n.name, err)
	}
	defer conn.Close(ctx)
	var v string
	if err := conn.QueryRow(ctx, sql).Scan(&v); err != nil {
		n.t.Fatalf("%s: %s: %v", n.name, sql, err)
	}
	return v
}

func (n *pgNode) inRecovery() bool {
	n.t.Helper()
	return n.query1("SELECT pg_is_in_recovery()::text") == "true"
}

// rowCounts returns how many times each req_id appears in writes.
func (n *pgNode) rowCounts() map[int64]int {
	n.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := n.connect(ctx)
	if err != nil {
		n.t.Fatalf("%s: connect: %v", n.name, err)
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, "SELECT req_id, count(*) FROM writes GROUP BY req_id")
	if err != nil {
		n.t.Fatalf("%s: rowCounts: %v", n.name, err)
	}
	defer rows.Close()
	counts := map[int64]int{}
	for rows.Next() {
		var id, c int64
		if err := rows.Scan(&id, &c); err != nil {
			n.t.Fatalf("%s: scan: %v", n.name, err)
		}
		counts[id] = int(c)
	}
	if err := rows.Err(); err != nil {
		n.t.Fatalf("%s: rowCounts: %v", n.name, err)
	}
	return counts
}

// cluster is a primary with one real streaming replica.
type cluster struct {
	primary, replica *pgNode
}

// newCluster initializes a primary, creates the workload table, clones a
// streaming replica from it with pg_basebackup, starts both, and waits for
// the replica's WAL receiver to be streaming. Everything is torn down (hard)
// when the test ends, and each node's server log is printed if it failed.
func newCluster(t *testing.T, tools pgTools) *cluster {
	t.Helper()
	root := t.TempDir() // registered first, so it is removed last, after the servers stop

	primary := &pgNode{t: t, tools: tools, name: "primary", dir: filepath.Join(root, "primary"), port: freePort(t)}
	replica := &pgNode{t: t, tools: tools, name: "replica", dir: filepath.Join(root, "replica"), port: freePort(t)}

	tools.mustRun(t, "initdb", "-D", primary.dir, "-U", "postgres", "--auth=trust", "--no-locale", "-E", "UTF8")
	primary.start()
	t.Cleanup(primary.stopQuietly)
	// req_id is deliberately not unique: a client that retries a write that
	// had in fact committed produces a visible duplicate, which is exactly
	// what the harness wants to be able to count.
	primary.exec("CREATE TABLE writes (seq bigserial PRIMARY KEY, req_id bigint NOT NULL)")

	tools.mustRun(t, "pg_basebackup",
		"-h", "127.0.0.1", "-p", strconv.Itoa(primary.port), "-U", "postgres",
		"-D", replica.dir, "-X", "stream", "-R", "-w")
	replica.start()
	t.Cleanup(replica.stopQuietly)

	c := &cluster{primary: primary, replica: replica}
	deadline := time.Now().Add(30 * time.Second)
	for {
		if replica.query1("SELECT COALESCE((SELECT status FROM pg_stat_wal_receiver LIMIT 1), '')") == "streaming" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("replica's WAL receiver never started streaming")
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		for _, n := range []*pgNode{primary, replica} {
			t.Logf("---- tail of %s server log ----\n%s", n.name, tail(n.log(), 25))
		}
	})
	return c
}

func tail(path string, lines int) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Sprintf("(unreadable: %v)", err)
	}
	all := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(all) > lines {
		all = all[len(all)-lines:]
	}
	return strings.Join(all, "\n")
}
