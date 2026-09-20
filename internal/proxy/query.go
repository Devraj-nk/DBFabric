package proxy

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"dbfabric/internal/pool"
	"dbfabric/internal/router"
)

// shardKeyFromParams picks the shard key out of the client's startup
// parameters.
//
// TODO: this is an interim convention, not a real routing-hint
// mechanism — general SQL doesn't carry a shard key, so for now we
// require the client to supply one via application_name (falling back
// to user). A production version would want an explicit convention
// agreed with callers (a dedicated startup param, a schema-based
// scheme, or a required leading comment), same as real sharding
// proxies restrict what they can route without a hint.
func shardKeyFromParams(params map[string]string) string {
	if v := params["application_name"]; v != "" {
		return v
	}
	return params["user"]
}

// consistencyHintRe matches a leading SQL comment naming a
// consistency level, e.g. "-- consistency=eventual" or
// "-- consistency=bounded_staleness:500" (the ":500" is a max-lag-ms
// override for bounded staleness; ignored for the other levels).
var consistencyHintRe = regexp.MustCompile(`(?i)^--\s*consistency\s*=\s*(\w+)(?::(\d+))?`)

// consistencyFromQuery reads the same leading-comment convention for
// the read consistency level (and, for bounded staleness, its max-lag
// threshold in ms), defaulting to Strong. A bounded-staleness hint that
// carries no ":<ms>" suffix gets defaultMaxLagMS (from config). Like
// shardKeyFromParams, this is a placeholder convention, not SQL
// parsing — see the router.Consistency doc comment for what each
// level means.
func consistencyFromQuery(query string, defaultMaxLagMS int64) (router.Consistency, int64) {
	m := consistencyHintRe.FindStringSubmatch(strings.TrimSpace(query))
	if m == nil {
		return router.Strong, 0
	}
	switch c := router.Consistency(strings.ToLower(m[1])); c {
	case router.Eventual:
		return router.Eventual, 0
	case router.BoundedStaleness:
		maxLagMS := defaultMaxLagMS
		if m[2] != "" {
			if v, err := strconv.ParseInt(m[2], 10, 64); err == nil {
				maxLagMS = v
			}
		}
		return router.BoundedStaleness, maxLagMS
	default:
		return router.Strong, 0
	}
}

// handleQuery resolves shardKey/consistency to a target node and
// forwards query to it over a pooled backend connection, streaming
// the real result back to the client. Errors from resolution or from
// the backend both surface as a Postgres ErrorResponse rather than
// closing the connection — a bad query on one connection shouldn't
// take the session down.
func handleQuery(ctx context.Context, w *bufio.ReadWriter, rt *router.Router, pm *pool.Manager, defaultMaxLagMS int64, shardKey, query string) error {
	if isTransactionControl(query) {
		return writeErrorResponse(w, "ERROR", "0A000", errTransactionsUnsupported)
	}

	consistency, maxLagMS := consistencyFromQuery(query, defaultMaxLagMS)

	node, err := rt.Resolve(shardKey, consistency, maxLagMS)
	if err != nil {
		return writeErrorResponse(w, "ERROR", "XX000", err.Error())
	}

	backend, err := pm.Get(ctx, node.Addr)
	if err != nil {
		return writeErrorResponse(w, "ERROR", "08006", fmt.Sprintf("connecting to backend %s: %v", node.Addr, err))
	}

	rows, err := backend.Query(ctx, query)
	if err != nil {
		return pgErrorResponse(w, err)
	}
	defer rows.Close()

	return streamResult(w, rows)
}

const errTransactionsUnsupported = "transaction control (BEGIN/COMMIT/ROLLBACK/SAVEPOINT) is not supported by this proxy: " +
	"every query runs on a separate pooled backend connection, so the statements between BEGIN and COMMIT would not be atomic " +
	"(a ROLLBACK would silently leave them committed)"

// isTransactionControl reports whether query's first statement keyword is
// one that opens, closes, or subdivides a transaction.
//
// These are refused rather than forwarded because forwarding them is worse
// than failing: each query is executed on whatever pooled connection is
// free, and pgx's pool destroys a connection released with a transaction
// still open. So BEGIN "succeeds" and is then lost, the following INSERT
// runs in autocommit, and a later ROLLBACK is a no-op — the client believes
// it rolled its write back and it was committed. Real transaction support
// needs a backend connection pinned to the client for the transaction's
// duration; until then, refusing loudly is the only safe behavior.
func isTransactionControl(query string) bool {
	switch leadingKeyword(query) {
	case "BEGIN", "START", "COMMIT", "END", "ROLLBACK", "ABORT", "SAVEPOINT", "RELEASE":
		return true
	}
	return false
}

// leadingKeyword returns the first SQL keyword of query, uppercased,
// skipping leading whitespace and comments (so a "-- consistency=..."
// routing hint doesn't hide the statement behind it). It returns "" if
// the query has no leading word.
func leadingKeyword(query string) string {
	s := query
	for {
		s = strings.TrimLeft(s, " \t\r\n")
		switch {
		case strings.HasPrefix(s, "--"):
			nl := strings.IndexByte(s, '\n')
			if nl < 0 {
				return ""
			}
			s = s[nl+1:]
		case strings.HasPrefix(s, "/*"):
			end := strings.Index(s, "*/")
			if end < 0 {
				return ""
			}
			s = s[end+2:]
		default:
			end := 0
			for end < len(s) && (s[end] >= 'a' && s[end] <= 'z' || s[end] >= 'A' && s[end] <= 'Z') {
				end++
			}
			return strings.ToUpper(s[:end])
		}
	}
}

// streamResult writes rows to the client: RowDescription and DataRows for
// a statement that returns columns, then CommandComplete — or an
// ErrorResponse if the statement failed.
//
// It must consume rows to the end even when the statement returns no
// columns (INSERT, UPDATE, ...). pgx only reports the outcome once the
// rows are closed: Err() "may return nil even if the query failed on the
// server" before then, and CommandTag() "is only available after Rows is
// closed". Skipping the drain for column-less statements would acknowledge
// a write before it had actually completed — which is exactly what an
// earlier version of this function did, and the chaos harness caught it as
// acknowledged writes missing from a primary that never crashed.
func streamResult(w io.Writer, rows pgx.Rows) error {
	fields := rows.FieldDescriptions()
	hasColumns := len(fields) > 0
	if hasColumns {
		columns := make([]string, len(fields))
		for i, f := range fields {
			columns[i] = f.Name
		}
		if err := writeRowDescription(w, columns); err != nil {
			return err
		}
	}

	for rows.Next() {
		if !hasColumns {
			continue
		}
		values, err := rows.Values()
		if err != nil {
			return pgErrorResponse(w, err)
		}
		if err := writeDataRow(w, values); err != nil {
			return err
		}
	}
	// Next() returning false closed the rows, so Err and CommandTag are
	// now authoritative.
	if err := rows.Err(); err != nil {
		return pgErrorResponse(w, err)
	}
	return writeCommandComplete(w, rows.CommandTag().String())
}

// pgErrorResponse writes err as an ErrorResponse, using the backend's
// own SQLSTATE and message when err came from Postgres (a *pgconn.PgError)
// rather than re-wrapping it — psql (and other real clients) prefix
// the message with the severity themselves, so passing pgErr.Message
// instead of err.Error() (which already contains "ERROR: ...") avoids
// showing the severity twice.
func pgErrorResponse(w io.Writer, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return writeErrorResponse(w, "ERROR", pgErr.Code, pgErr.Message)
	}
	return writeErrorResponse(w, "ERROR", "XX000", err.Error())
}
