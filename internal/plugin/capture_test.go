package plugin

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/mars160/cpa-distill/internal/storage"
)

func TestApplySummaryAttributes(t *testing.T) {
	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	var c storage.Capture
	env := hookEnvelope{
		Model:          json.RawMessage(`"upstream-a"`),
		RequestedModel: json.RawMessage(`"alias-a"`),
		Stream:         json.RawMessage(`true`),
		SourceFormat:   json.RawMessage(`"openai"`),
	}
	applySummary(&c, &env, now)
	if c.Model != "upstream-a" || c.RequestedModel != "alias-a" || !c.Stream || c.SourceFormat != "openai" || !c.StartedAt.Equal(now) {
		t.Fatalf("unexpected capture: %+v", c)
	}
	// A malformed optional attribute is ignored; it must not erase earlier values.
	env = hookEnvelope{Model: json.RawMessage(`123`), RequestedModel: json.RawMessage(`"alias-b"`)}
	applySummary(&c, &env, now.Add(time.Second))
	if c.Model != "upstream-a" || c.RequestedModel != "alias-b" || !c.Stream || !c.StartedAt.Equal(now) {
		t.Fatalf("merge lost prior attributes: %+v", c)
	}
	env = hookEnvelope{StartedAt: json.RawMessage(`"2026-10-15T11:59:59Z"`), CompletedAt: json.RawMessage(`"2026-10-15T12:00:01Z"`)}
	applySummary(&c, &env, now.Add(time.Second))
	if !c.StartedAt.Equal(now.Add(-time.Second)) || !c.CompletedAt.Equal(now.Add(time.Second)) {
		t.Fatalf("terminal timestamps ignored: %+v", c)
	}
}

func TestHookEnvelopeBodyDecoding(t *testing.T) {
	raw := mustMarshal(map[string]any{"RequestID": "r", "Body": []byte{0x00, 0xff, 0x10}, "ChunkIndex": 0, "Payload": []byte("ws")})
	var env hookEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatal(err)
	}
	body, err := decodeBodyBytes(env.Body)
	if err != nil || !bytes.Equal(body, []byte{0x00, 0xff, 0x10}) {
		t.Fatalf("body = %q (err %v), want byte-exact base64 decode", body, err)
	}
	payload, err := decodeBodyBytes(env.Payload)
	if err != nil || string(payload) != "ws" {
		t.Fatalf("payload = %q (err %v)", payload, err)
	}
	if index, ok, err := decodeChunkIndex(env.ChunkIndex); err != nil || !ok || index != 0 {
		t.Fatalf("chunk index = (%d,%v,%v), want (0,true,nil)", index, ok, err)
	}
	// Absent fields are not an error and yield no bytes.
	if b, err := decodeBodyBytes(nil); err != nil || b != nil {
		t.Fatalf("absent body = %q (err %v), want nil", b, err)
	}
	if index, ok, err := decodeChunkIndex(nil); err != nil || ok || index != 0 {
		t.Fatalf("absent chunk index = (%d,%v,%v), want (0,false,nil)", index, ok, err)
	}
	// A present but malformed body or chunk index must be an error, so the caller
	// drops the whole capture instead of storing a truncated body.
	for _, bad := range []json.RawMessage{json.RawMessage(`"!!!"`), json.RawMessage(`42`), json.RawMessage(`{}`)} {
		if _, err := decodeBodyBytes(bad); err == nil {
			t.Errorf("malformed body %s was accepted", bad)
		}
	}
	if _, _, err := decodeChunkIndex(json.RawMessage(`"zero"`)); err == nil {
		t.Error("a text chunk index was accepted")
	}
	if index, ok, err := decodeChunkIndex(json.RawMessage(`-1`)); err != nil || !ok || index != -1 {
		t.Fatalf("negative chunk index = (%d,%v,%v), want (-1,true,nil)", index, ok, err)
	}
	// A JSON-escaped string keeps exact encoding/json semantics.
	if body, err := decodeBodyBytes(json.RawMessage(`"QUFB\u0051Q=="`)); err != nil || string(body) != "AAAA" {
		t.Fatalf("escaped body = %q (err %v), want AAAA", body, err)
	}
}

func TestDecodedBodyLenAccountsForPadding(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want int
	}{
		{`"AAAA"`, 3},
		{`"AAA="`, 2},
		{`"AA=="`, 1},
		{`""`, 0},
		{`null`, 0},
		{`42`, 0},
		{`{}`, 0},
		// A JSON escape defeats the length shortcut: no estimate, so the exact
		// limit applies after decoding and the body is never rejected on raw length.
		{`"QUFB\u0051Q=="`, 0},
	} {
		if got := decodedBodyLen(json.RawMessage(tc.raw)); got != tc.want {
			t.Errorf("decodedBodyLen(%s) = %d, want %d", tc.raw, got, tc.want)
		}
	}
	// The estimate must be exact for canonical base64 of every length: an
	// overestimate would reject a capture that fits its byte budget.
	for n := 0; n <= 12; n++ {
		encoded := base64.StdEncoding.EncodeToString(make([]byte, n))
		raw := append(append([]byte{'"'}, encoded...), '"')
		if got := decodedBodyLen(json.RawMessage(raw)); got != n {
			t.Errorf("decodedBodyLen(base64(%d bytes)) = %d, want %d", n, got, n)
		}
	}
}

func TestBodyGrowthMirrorsAccumulation(t *testing.T) {
	// "QUFBQQ==" decodes to "AAAA" (4 bytes).
	env := hookEnvelope{Body: json.RawMessage(`"QUFBQQ=="`), Payload: json.RawMessage(`"QUFBQQ=="`)}
	if got := bodyGrowth("request.intercept_before", &env, 0, false, &pending{}); got != 4 {
		t.Errorf("first before growth = %d, want 4", got)
	}
	if got := bodyGrowth("request.intercept_before", &env, 0, false, &pending{requestSeen: true}); got != 0 {
		t.Errorf("repeated before growth = %d, want 0", got)
	}
	if got := bodyGrowth("request.intercept_after", &env, 0, false, &pending{}); got != 0 {
		t.Errorf("after growth = %d, want 0", got)
	}
	if got := bodyGrowth("response.intercept_after", &env, 0, false, &pending{responseBody: []byte("xx")}); got != 2 {
		t.Errorf("response replacement growth = %d, want 2", got)
	}
	if got := bodyGrowth("response.intercept_stream_chunk", &env, -1, true, &pending{}); got != 0 {
		t.Errorf("stream init growth = %d, want 0", got)
	}
	if got := bodyGrowth("response.intercept_stream_chunk", &env, 0, true, &pending{}); got != 4 {
		t.Errorf("stream chunk growth = %d, want 4", got)
	}
	if got := bodyGrowth("websocket.response_event", &env, 0, false, &pending{}); got != 4 {
		t.Errorf("first websocket growth = %d, want 4", got)
	}
	if got := bodyGrowth("websocket.response_event", &env, 0, false, &pending{websocketBody: []byte("a")}); got != 5 {
		t.Errorf("websocket growth with separator = %d, want 5", got)
	}
}

func TestClientKeyIdentifier(t *testing.T) {
	if keyID("") != "unknown" {
		t.Fatal("missing key must have explicit unknown identity")
	}
	a, b := keyID("sk-client-a"), keyID("sk-client-b")
	if len(a) != 64 || a == b || a != keyID("sk-client-a") {
		t.Fatal("identifiers must be stable, distinct SHA256 values")
	}
}
