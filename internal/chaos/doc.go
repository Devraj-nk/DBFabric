// Package chaos is DBFabric's failure-injection harness: it builds a real
// two-node PostgreSQL cluster (a primary and a streaming replica), runs the
// proxy in-process in front of it, drives a write workload through the
// proxy, injects failures, and measures what happened.
//
// The tests need PostgreSQL server binaries (initdb, pg_ctl, pg_basebackup)
// and take a minute or two, so they sit behind a build tag and are not part
// of `go test ./...`:
//
//	go test -tags chaos -v -timeout 15m ./internal/chaos
//
// Binaries are found via the PG_BIN environment variable, then PATH, then
// the default Windows install location; the tests skip if none is found.
//
// Scenarios:
//   - kill the primary in the middle of a write burst, and measure recovery
//     time and lost/duplicated writes
//   - kill a replica, and check that writes are unaffected and reads stop
//     being routed to it
//   - partition the proxy from a primary that is still alive, with the
//     split-brain guard on (no promotion) and off (two writable primaries)
package chaos
