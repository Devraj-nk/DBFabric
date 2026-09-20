package proxy

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"dbfabric/internal/router"
)

// fakeRows models the part of pgx.Rows's contract that streamResult has to
// respect: the outcome of the statement (its error and command tag) only
// becomes visible once the rows have been consumed to the end and closed.
// Before that, Err() reports nil even for a statement that has failed — so
// code that reads the outcome without draining the rows sees success.
type fakeRows struct {
	pgx.Rows // methods this fake doesn't override panic if reached

	fields []pgconn.FieldDescription
	data   [][]any
	pos    int
	closed bool

	failAtCompletion error // becomes visible only after close
	tag              string
}

func (f *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return f.fields }

func (f *fakeRows) Next() bool {
	if f.pos < len(f.data) {
		f.pos++
		return true
	}
	f.closed = true
	return false
}

func (f *fakeRows) Values() ([]any, error) { return f.data[f.pos-1], nil }

func (f *fakeRows) Err() error {
	if !f.closed {
		return nil
	}
	return f.failAtCompletion
}

func (f *fakeRows) CommandTag() pgconn.CommandTag {
	if !f.closed {
		return pgconn.CommandTag{}
	}
	return pgconn.NewCommandTag(f.tag)
}

func (f *fakeRows) Close() { f.closed = true }

// messageTypes decodes every backend message in buf and returns their types
// in order along with the payload of each.
func messageTypes(t *testing.T, buf *bytes.Buffer) (types string, payloads [][]byte) {
	t.Helper()
	for buf.Len() > 0 {
		typ, payload, err := readMessage(buf)
		if err != nil {
			t.Fatalf("decoding backend messages: %v", err)
		}
		types += string(typ)
		payloads = append(payloads, payload)
	}
	return types, payloads
}

func TestStreamResultColumnlessSuccessReportsTheRealTag(t *testing.T) {
	rows := &fakeRows{tag: "INSERT 0 1"}
	var out bytes.Buffer
	if err := streamResult(&out, rows); err != nil {
		t.Fatal(err)
	}
	types, payloads := messageTypes(t, &out)
	if types != "C" {
		t.Fatalf("messages = %q, want just CommandComplete (no RowDescription for a column-less statement)", types)
	}
	if got := strings.TrimRight(string(payloads[0]), "\x00"); got != "INSERT 0 1" {
		t.Errorf("command tag = %q, want %q (an empty tag means CommandTag was read before the rows were drained)", got, "INSERT 0 1")
	}
}

// The regression: a column-less statement that fails only when it completes
// must produce an ErrorResponse, never CommandComplete. Acknowledging it
// tells the client a write succeeded that did not.
func TestStreamResultColumnlessFailureIsNotAcknowledged(t *testing.T) {
	rows := &fakeRows{failAtCompletion: errors.New("connection reset before commit"), tag: "INSERT 0 1"}
	var out bytes.Buffer
	if err := streamResult(&out, rows); err != nil {
		t.Fatal(err)
	}
	types, _ := messageTypes(t, &out)
	if types != "E" {
		t.Fatalf("messages = %q, want a single ErrorResponse: the failed write must not be acknowledged", types)
	}
}

func TestStreamResultRowsThenCommandComplete(t *testing.T) {
	rows := &fakeRows{
		fields: []pgconn.FieldDescription{{Name: "a"}},
		data:   [][]any{{int64(1)}, {int64(2)}},
		tag:    "SELECT 2",
	}
	var out bytes.Buffer
	if err := streamResult(&out, rows); err != nil {
		t.Fatal(err)
	}
	types, payloads := messageTypes(t, &out)
	if types != "TDDC" {
		t.Fatalf("messages = %q, want RowDescription, 2 DataRows, CommandComplete", types)
	}
	if got := strings.TrimRight(string(payloads[3]), "\x00"); got != "SELECT 2" {
		t.Errorf("command tag = %q, want %q", got, "SELECT 2")
	}
}

func TestStreamResultFailureAfterRowsEndsInErrorNotCommandComplete(t *testing.T) {
	rows := &fakeRows{
		fields:           []pgconn.FieldDescription{{Name: "a"}},
		data:             [][]any{{int64(1)}},
		failAtCompletion: errors.New("boom mid-result"),
	}
	var out bytes.Buffer
	if err := streamResult(&out, rows); err != nil {
		t.Fatal(err)
	}
	types, _ := messageTypes(t, &out)
	if types != "TDE" {
		t.Fatalf("messages = %q, want RowDescription, DataRow, then ErrorResponse (no CommandComplete)", types)
	}
}

func TestConsistencyFromQuery(t *testing.T) {
	const def = 750
	tests := []struct {
		name        string
		query       string
		wantLevel   router.Consistency
		wantMaxLagM int64
	}{
		{"no hint defaults to strong", "SELECT 1", router.Strong, 0},
		{"eventual", "-- consistency=eventual\nSELECT 1", router.Eventual, 0},
		{"eventual ignores a lag suffix", "-- consistency=eventual:50\nSELECT 1", router.Eventual, 0},
		{"bounded uses the configured default", "-- consistency=bounded_staleness\nSELECT 1", router.BoundedStaleness, def},
		{"bounded honors an explicit ms", "-- consistency=bounded_staleness:500\nSELECT 1", router.BoundedStaleness, 500},
		{"explicit zero is honored, not defaulted", "-- consistency=bounded_staleness:0\nSELECT 1", router.BoundedStaleness, 0},
		{"hint is case-insensitive", "-- CONSISTENCY = Eventual\nSELECT 1", router.Eventual, 0},
		{"leading whitespace tolerated", "  \n  -- consistency=eventual\nSELECT 1", router.Eventual, 0},
		{"unknown level falls back to strong", "-- consistency=sometimes\nSELECT 1", router.Strong, 0},
		{"hint not at the start is ignored", "SELECT 1 -- consistency=eventual", router.Strong, 0},
		{"strong spelled out", "-- consistency=strong\nSELECT 1", router.Strong, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			level, maxLag := consistencyFromQuery(tt.query, def)
			if level != tt.wantLevel {
				t.Errorf("level = %q, want %q", level, tt.wantLevel)
			}
			if maxLag != tt.wantMaxLagM {
				t.Errorf("maxLagMS = %d, want %d", maxLag, tt.wantMaxLagM)
			}
		})
	}
}

func TestIsTransactionControl(t *testing.T) {
	tests := []struct {
		query string
		want  bool
	}{
		{"BEGIN", true},
		{"begin", true},
		{"  Begin ;", true},
		{"BEGIN ISOLATION LEVEL SERIALIZABLE", true},
		{"START TRANSACTION", true},
		{"COMMIT", true},
		{"END", true},
		{"ROLLBACK", true},
		{"ROLLBACK TO SAVEPOINT a", true},
		{"ABORT", true},
		{"SAVEPOINT a", true},
		{"RELEASE SAVEPOINT a", true},
		{"-- consistency=eventual\nBEGIN", true}, // the routing hint must not hide it
		{"/* app tag */ COMMIT", true},           // nor a block comment
		{"-- one\n-- two\n  \n  rollback", true}, // nor several comments
		{"SELECT 1", false},
		{"INSERT INTO t VALUES (1)", false},
		{"-- consistency=eventual\nSELECT 1", false},
		{"SELECT 'BEGIN'", false},                  // only the first keyword counts
		{"DO $$ BEGIN PERFORM 1; END $$", false},   // a DO block is not transaction control
		{"CREATE TABLE beginnings (i int)", false}, // a word merely starting with BEGIN is not BEGIN
		{"", false},
		{"-- only a comment", false},
	}
	for _, tt := range tests {
		if got := isTransactionControl(tt.query); got != tt.want {
			t.Errorf("isTransactionControl(%q) = %v, want %v", tt.query, got, tt.want)
		}
	}
}

func TestShardKeyFromParams(t *testing.T) {
	tests := []struct {
		name   string
		params map[string]string
		want   string
	}{
		{"application_name wins", map[string]string{"application_name": "user-42", "user": "app"}, "user-42"},
		{"falls back to user", map[string]string{"user": "app"}, "app"},
		{"empty application_name falls back to user", map[string]string{"application_name": "", "user": "app"}, "app"},
		{"nothing set", map[string]string{}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shardKeyFromParams(tt.params); got != tt.want {
				t.Errorf("shardKeyFromParams = %q, want %q", got, tt.want)
			}
		})
	}
}
