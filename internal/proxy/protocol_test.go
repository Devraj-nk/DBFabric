package proxy

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"net"
	"testing"
)

// buildStartupMessage builds a raw StartupMessage as a real client
// would send it: 4-byte length, protocol version, then alternating
// null-terminated key/value pairs, terminated by one more zero byte.
func buildStartupMessage(params map[string]string) []byte {
	var body bytes.Buffer
	verBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(verBuf, protoVersion3)
	body.Write(verBuf)
	for k, v := range params {
		body.Write(cString(k))
		body.Write(cString(v))
	}
	body.WriteByte(0)

	full := make([]byte, 4)
	binary.BigEndian.PutUint32(full, uint32(4+body.Len()))
	return append(full, body.Bytes()...)
}

func newPipeReadWriter() (client *bufio.ReadWriter, server *bufio.ReadWriter, close func()) {
	c, s := net.Pipe()
	client = bufio.NewReadWriter(bufio.NewReader(c), bufio.NewWriter(c))
	server = bufio.NewReadWriter(bufio.NewReader(s), bufio.NewWriter(s))
	return client, server, func() { c.Close(); s.Close() }
}

func TestReadStartupParams(t *testing.T) {
	client, server, closeConns := newPipeReadWriter()
	defer closeConns()

	msg := buildStartupMessage(map[string]string{
		"user":             "app",
		"database":         "appdb",
		"application_name": "user-42",
	})

	go func() {
		client.Write(msg)
		client.Flush()
	}()

	params, err := readStartupParams(server)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"user": "app", "database": "appdb", "application_name": "user-42"}
	for k, v := range want {
		if params[k] != v {
			t.Errorf("params[%q] = %q, want %q", k, params[k], v)
		}
	}
}

func TestReadStartupParams_SSLRequestThenStartup(t *testing.T) {
	client, server, closeConns := newPipeReadWriter()
	defer closeConns()

	sslReq := make([]byte, 8)
	binary.BigEndian.PutUint32(sslReq[0:4], 8)
	binary.BigEndian.PutUint32(sslReq[4:8], sslRequestCode)
	startup := buildStartupMessage(map[string]string{"user": "app"})

	go func() {
		client.Write(sslReq)
		client.Flush()
		resp, err := client.ReadByte()
		if err != nil || resp != 'N' {
			t.Errorf("expected 'N' in response to SSLRequest, got %q, err=%v", resp, err)
		}
		client.Write(startup)
		client.Flush()
	}()

	params, err := readStartupParams(server)
	if err != nil {
		t.Fatal(err)
	}
	if params["user"] != "app" {
		t.Errorf("params[user] = %q, want %q", params["user"], "app")
	}
}

func TestSplitCStrings(t *testing.T) {
	tests := []struct {
		name    string
		in      []byte
		want    []string
		wantErr bool
	}{
		{"empty", nil, nil, true},
		{"not zero terminated", []byte("a"), nil, true},
		{"single pair", append(append([]byte("k"), 0), append([]byte("v"), 0, 0)...), []string{"k", "v"}, false},
		{"only terminator", []byte{0}, nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := splitCStrings(tt.in)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("got[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestWriteReadMessageRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	if err := writeMessage(&buf, 'Q', cString("SELECT 1")); err != nil {
		t.Fatal(err)
	}
	msgType, payload, err := readMessage(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if msgType != 'Q' {
		t.Errorf("msgType = %q, want 'Q'", msgType)
	}
	if string(payload) != "SELECT 1\x00" {
		t.Errorf("payload = %q, want %q", payload, "SELECT 1\x00")
	}
}

// decodeDataRow parses a DataRow payload back into its column values,
// mirroring what a real client decoder would do.
func decodeDataRow(t *testing.T, payload []byte) []string {
	t.Helper()
	if len(payload) < 2 {
		t.Fatalf("DataRow payload too short: %d bytes", len(payload))
	}
	count := binary.BigEndian.Uint16(payload[0:2])
	offset := 2
	values := make([]string, 0, count)
	for i := 0; i < int(count); i++ {
		if offset+4 > len(payload) {
			t.Fatalf("DataRow payload truncated at column %d", i)
		}
		l := binary.BigEndian.Uint32(payload[offset : offset+4])
		offset += 4
		if offset+int(l) > len(payload) {
			t.Fatalf("DataRow payload truncated in column %d value", i)
		}
		values = append(values, string(payload[offset:offset+int(l)]))
		offset += int(l)
	}
	return values
}

func TestWriteDataRow(t *testing.T) {
	var buf bytes.Buffer
	if err := writeDataRow(&buf, []any{"a", "bb", "ccc"}); err != nil {
		t.Fatal(err)
	}
	msgType, payload, err := readMessage(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if msgType != 'D' {
		t.Fatalf("msgType = %q, want 'D'", msgType)
	}
	got := decodeDataRow(t, payload)
	want := []string{"a", "bb", "ccc"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("got[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestWriteDataRow_NullAndTypedValues(t *testing.T) {
	var buf bytes.Buffer
	if err := writeDataRow(&buf, []any{nil, true, false, 42, "hi"}); err != nil {
		t.Fatal(err)
	}
	_, payload, err := readMessage(&buf)
	if err != nil {
		t.Fatal(err)
	}

	count := binary.BigEndian.Uint16(payload[0:2])
	if count != 5 {
		t.Fatalf("column count = %d, want 5", count)
	}
	offset := 2

	// nil -> SQL NULL: length -1, no bytes.
	length := binary.BigEndian.Uint32(payload[offset : offset+4])
	if int32(length) != -1 {
		t.Errorf("nil value encoded length = %d, want -1", int32(length))
	}
	offset += 4

	readNext := func() string {
		l := binary.BigEndian.Uint32(payload[offset : offset+4])
		offset += 4
		v := string(payload[offset : offset+int(l)])
		offset += int(l)
		return v
	}

	if v := readNext(); v != "t" {
		t.Errorf("true encoded as %q, want %q", v, "t")
	}
	if v := readNext(); v != "f" {
		t.Errorf("false encoded as %q, want %q", v, "f")
	}
	if v := readNext(); v != "42" {
		t.Errorf("42 encoded as %q, want %q", v, "42")
	}
	if v := readNext(); v != "hi" {
		t.Errorf(`"hi" encoded as %q, want %q`, v, "hi")
	}
}
