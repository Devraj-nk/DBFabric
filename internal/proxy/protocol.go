package proxy

// Minimal Postgres frontend/backend protocol (v3) primitives — just
// enough to complete the startup handshake and read/write the simple
// query subprotocol. Not a general client library: no COPY, no
// extended query protocol, no auth beyond "trust". See
// https://www.postgresql.org/docs/current/protocol-message-formats.html.

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

const (
	protoVersion3     = 196608   // 3.0, the version field of a real StartupMessage
	sslRequestCode    = 80877103 // sent instead of StartupMessage when sslmode requests TLS
	gssEncRequestCode = 80877104 // ditto, for GSS encryption
)

// readStartupParams performs the pre-authentication handshake: it
// answers any SSLRequest/GSSENCRequest with a plain "no" (this proxy
// doesn't terminate TLS itself) and returns the parsed StartupMessage
// parameters (user, database, application_name, ...).
func readStartupParams(rw *bufio.ReadWriter) (map[string]string, error) {
	for {
		body, err := readUntyped(rw.Reader)
		if err != nil {
			return nil, err
		}
		if len(body) < 4 {
			return nil, fmt.Errorf("proxy: startup message too short (%d bytes)", len(body))
		}
		code := binary.BigEndian.Uint32(body[:4])

		switch code {
		case sslRequestCode, gssEncRequestCode:
			if _, err := rw.Write([]byte{'N'}); err != nil {
				return nil, err
			}
			if err := rw.Flush(); err != nil {
				return nil, err
			}
			continue // the client sends its real StartupMessage next
		case protoVersion3:
			return parseStartupParams(body[4:])
		default:
			return nil, fmt.Errorf("proxy: unsupported startup code %d", code)
		}
	}
}

// readUntyped reads a pre-authentication message: a 4-byte big-endian
// length (inclusive of itself), followed by (length-4) bytes of body.
// Unlike post-auth messages, these have no leading type byte.
func readUntyped(r io.Reader) (body []byte, err error) {
	var lenBuf [4]byte
	if _, err = io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lenBuf[:])
	if length < 4 {
		return nil, fmt.Errorf("proxy: invalid message length %d", length)
	}
	body = make([]byte, length-4)
	if _, err = io.ReadFull(r, body); err != nil {
		return nil, err
	}
	return body, nil
}

// parseStartupParams splits a StartupMessage's param section (the
// bytes after the protocol version) into key/value pairs: alternating
// null-terminated strings, the whole run ending with one more zero byte.
func parseStartupParams(body []byte) (map[string]string, error) {
	strs, err := splitCStrings(body)
	if err != nil {
		return nil, err
	}
	params := make(map[string]string, len(strs)/2)
	for i := 0; i+1 < len(strs); i += 2 {
		params[strs[i]] = strs[i+1]
	}
	return params, nil
}

// splitCStrings splits a run of null-terminated strings (the run
// itself terminated by one extra trailing zero byte) into its
// component strings.
func splitCStrings(body []byte) ([]string, error) {
	if len(body) == 0 || body[len(body)-1] != 0 {
		return nil, fmt.Errorf("proxy: param list not zero-terminated")
	}
	body = body[:len(body)-1] // drop the run's final terminator
	if len(body) == 0 {
		return nil, nil
	}
	var out []string
	start := 0
	for i, b := range body {
		if b == 0 {
			out = append(out, string(body[start:i]))
			start = i + 1
		}
	}
	return out, nil
}

// readMessage reads one post-authentication frontend/backend message:
// a 1-byte type, a 4-byte big-endian length (inclusive of itself), and
// (length-4) bytes of payload.
func readMessage(r io.Reader) (msgType byte, payload []byte, err error) {
	var typeBuf [1]byte
	if _, err = io.ReadFull(r, typeBuf[:]); err != nil {
		return 0, nil, err
	}
	var lenBuf [4]byte
	if _, err = io.ReadFull(r, lenBuf[:]); err != nil {
		return 0, nil, err
	}
	length := binary.BigEndian.Uint32(lenBuf[:])
	if length < 4 {
		return 0, nil, fmt.Errorf("proxy: invalid message length %d", length)
	}
	payload = make([]byte, length-4)
	if _, err = io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	return typeBuf[0], payload, nil
}

// writeMessage writes one typed message: type byte, big-endian length
// (inclusive of itself), then payload.
func writeMessage(w io.Writer, msgType byte, payload []byte) error {
	buf := make([]byte, 5, 5+len(payload))
	buf[0] = msgType
	binary.BigEndian.PutUint32(buf[1:5], uint32(4+len(payload)))
	buf = append(buf, payload...)
	_, err := w.Write(buf)
	return err
}

func cString(s string) []byte {
	return append([]byte(s), 0)
}

func writeAuthenticationOK(w io.Writer) error {
	return writeMessage(w, 'R', make([]byte, 4)) // 0 = AuthenticationOk
}

func writeParameterStatus(w io.Writer, key, value string) error {
	payload := append(cString(key), cString(value)...)
	return writeMessage(w, 'S', payload)
}

func writeBackendKeyData(w io.Writer, pid, secret uint32) error {
	payload := make([]byte, 8)
	binary.BigEndian.PutUint32(payload[0:4], pid)
	binary.BigEndian.PutUint32(payload[4:8], secret)
	return writeMessage(w, 'K', payload)
}

func writeReadyForQuery(w io.Writer, status byte) error {
	return writeMessage(w, 'Z', []byte{status})
}

// writeErrorResponse writes a minimal ErrorResponse: severity (e.g.
// "ERROR"), a SQLSTATE code (e.g. "XX000"), and a human-readable
// message.
func writeErrorResponse(w io.Writer, severity, code, message string) error {
	var payload []byte
	payload = append(payload, 'S')
	payload = append(payload, cString(severity)...)
	payload = append(payload, 'C')
	payload = append(payload, cString(code)...)
	payload = append(payload, 'M')
	payload = append(payload, cString(message)...)
	payload = append(payload, 0) // terminates the field list
	return writeMessage(w, 'E', payload)
}

func writeCommandComplete(w io.Writer, tag string) error {
	return writeMessage(w, 'C', cString(tag))
}

// writeRowDescription writes a RowDescription naming each column as
// an untyped text field — good enough for the routing-decision rows
// this proxy currently returns (see query.go), not a general-purpose
// result encoder.
func writeRowDescription(w io.Writer, columns []string) error {
	payload := make([]byte, 2)
	binary.BigEndian.PutUint16(payload, uint16(len(columns)))
	for _, col := range columns {
		payload = append(payload, cString(col)...)
		field := make([]byte, 18)
		// tableOID=0, colAttrNum=0, typeOID=25 (text), typeLen=-1 (variable), typeMod=-1, format=0 (text)
		binary.BigEndian.PutUint32(field[6:10], 25)
		binary.BigEndian.PutUint16(field[10:12], 0xFFFF) // int16(-1): variable-length type
		binary.BigEndian.PutUint32(field[12:16], 0xFFFFFFFF) // int32(-1): no type modifier
		payload = append(payload, field...)
	}
	return writeMessage(w, 'T', payload)
}

// writeDataRow writes a DataRow for arbitrary Go values (as returned
// by pgx's Rows.Values()), encoding each as Postgres text format. A
// nil value encodes as SQL NULL (length -1, no bytes) rather than an
// empty string — the two are distinct in the wire protocol.
func writeDataRow(w io.Writer, values []any) error {
	payload := make([]byte, 2)
	binary.BigEndian.PutUint16(payload, uint16(len(values)))
	for _, v := range values {
		if v == nil {
			payload = append(payload, 0xFF, 0xFF, 0xFF, 0xFF) // -1: SQL NULL
			continue
		}
		text := formatPGValue(v)
		lenBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(lenBuf, uint32(len(text)))
		payload = append(payload, lenBuf...)
		payload = append(payload, text...)
	}
	return writeMessage(w, 'D', payload)
}

// formatPGValue renders a decoded column value the way Postgres's own
// text format would, for the common cases this proxy is likely to see.
// Not a complete implementation of Postgres's text output rules for
// every type (arrays, composite types, etc. fall through to
// fmt.Sprintf, which won't always match exactly).
func formatPGValue(v any) string {
	switch val := v.(type) {
	case bool:
		if val {
			return "t"
		}
		return "f"
	case []byte:
		return string(val)
	case time.Time:
		return val.Format("2006-01-02 15:04:05.999999-07:00")
	case fmt.Stringer:
		return val.String()
	default:
		return fmt.Sprintf("%v", val)
	}
}
