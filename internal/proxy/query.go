package proxy

import (
	"bufio"
	"regexp"
	"strings"

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
// consistency level, e.g. "-- consistency=eventual".
var consistencyHintRe = regexp.MustCompile(`(?i)^--\s*consistency\s*=\s*(\S+)`)

// consistencyFromQuery reads the same leading-comment convention for
// the read consistency level, defaulting to Strong. Like
// shardKeyFromParams, this is a placeholder convention, not SQL
// parsing — see the router.Consistency doc comment for what each
// level means.
func consistencyFromQuery(query string) router.Consistency {
	m := consistencyHintRe.FindStringSubmatch(strings.TrimSpace(query))
	if m == nil {
		return router.Strong
	}
	switch c := router.Consistency(strings.ToLower(m[1])); c {
	case router.Eventual, router.BoundedStaleness:
		return c
	default:
		return router.Strong
	}
}

// handleQuery resolves shardKey/consistency to a target node and
// reports the routing decision back to the client as a one-row result
// set.
//
// TODO: once internal/pool holds real backend connections (pgx), stop
// reporting the decision and actually forward `query` to `node`,
// streaming its response back instead.
func handleQuery(w *bufio.ReadWriter, rt *router.Router, shardKey, query string) error {
	consistency := consistencyFromQuery(query)

	node, err := rt.Resolve(shardKey, consistency)
	if err != nil {
		return writeErrorResponse(w, "ERROR", "XX000", err.Error())
	}

	if err := writeRowDescription(w, []string{"shard_key", "consistency", "routed_to"}); err != nil {
		return err
	}
	if err := writeDataRow(w, []string{shardKey, string(consistency), node.Addr}); err != nil {
		return err
	}
	return writeCommandComplete(w, "SELECT 1")
}
