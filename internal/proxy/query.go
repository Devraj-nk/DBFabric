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

// defaultMaxLagMS is the bounded-staleness threshold used when a
// "-- consistency=bounded_staleness" hint doesn't specify one itself.
//
// TODO: not sourced from config — a fixed default is a reasonable
// starting point, but a real deployment would want this (and probably
// the whole hint mechanism) configurable rather than a constant.
const defaultMaxLagMS = 1000

// consistencyHintRe matches a leading SQL comment naming a
// consistency level, e.g. "-- consistency=eventual" or
// "-- consistency=bounded_staleness:500" (the ":500" is a max-lag-ms
// override for bounded staleness; ignored for the other levels).
var consistencyHintRe = regexp.MustCompile(`(?i)^--\s*consistency\s*=\s*(\w+)(?::(\d+))?`)

// consistencyFromQuery reads the same leading-comment convention for
// the read consistency level (and, for bounded staleness, its max-lag
// threshold in ms), defaulting to Strong. Like shardKeyFromParams,
// this is a placeholder convention, not SQL parsing — see the
// router.Consistency doc comment for what each level means.
func consistencyFromQuery(query string) (router.Consistency, int64) {
	m := consistencyHintRe.FindStringSubmatch(strings.TrimSpace(query))
	if m == nil {
		return router.Strong, 0
	}
	switch c := router.Consistency(strings.ToLower(m[1])); c {
	case router.Eventual:
		return router.Eventual, 0
	case router.BoundedStaleness:
		maxLagMS := int64(defaultMaxLagMS)
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
func handleQuery(ctx context.Context, w *bufio.ReadWriter, rt *router.Router, pm *pool.Manager, shardKey, query string) error {
	consistency, maxLagMS := consistencyFromQuery(query)

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

	fields := rows.FieldDescriptions()
	if len(fields) > 0 {
		columns := make([]string, len(fields))
		for i, f := range fields {
			columns[i] = f.Name
		}
		if err := writeRowDescription(w, columns); err != nil {
			return err
		}
		for rows.Next() {
			values, err := rows.Values()
			if err != nil {
				return pgErrorResponse(w, err)
			}
			if err := writeDataRow(w, values); err != nil {
				return err
			}
		}
	}
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
