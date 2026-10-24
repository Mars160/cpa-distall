package plugin

// Tests for the CLIProxyAPI C-ABI JSON contract implemented by Plugin.Handle.
//
// Contract under test (see scripts/smoke.py for the end-to-end reference):
//   * every hook response is exactly {"ok":true,"result":{}} - no traffic rewriting;
//   * only requests that start with request.intercept_before and finish with
//     request.complete/Outcome=succeeded are persisted, as one capture;
//   * the capture keeps the client's first request body, the response body (stream
//     chunks concatenated, ChunkIndex -1 excluded) and websocket payloads separately;
//   * persistence is asynchronous and must never block or fail a hook;
//   * credentials in headers/metadata never reach storage while base64 bodies do;
//   * buffer/queue/pending limits skip the whole request instead of a prefix.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mars160/cpa-distill/internal/storage"
)

var passThrough = []byte(`{"ok":true,"result":{}}`)

var hookMethods = []string{
	"request.intercept_before",
	"request.intercept_after",
	"response.intercept_after",
	"response.intercept_stream_chunk",
	"request.complete",
	"websocket.response_event",
}

// ---------------------------------------------------------------------------
// harness
// ---------------------------------------------------------------------------

type envelope struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result"`
	Error  struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type fakeCapture struct {
	Capture storage.Capture
	Bodies  storage.CaptureBodies
}

type fakeKey struct {
	ID    string
	Value string
}

// fakeSink records every capture and key handed to the background writer. It can
// be made to fail or to park inside AppendCapture until its gate is closed (or
// the caller-supplied context expires), which is how "database is slow" is
// modelled.
type fakeSink struct {
	mu       sync.Mutex
	captures []fakeCapture
	keys     []fakeKey
	closes   int
	calls    int
	keyCalls int
	err      error
	keyErr   error
	gate     chan struct{}
	entered  chan struct{}
}

func (f *fakeSink) AppendCapture(ctx context.Context, c storage.Capture, b storage.CaptureBodies) error {
	f.mu.Lock()
	f.calls++
	gate, err, entered := f.gate, f.err, f.entered
	f.mu.Unlock()
	if entered != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
	}
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.captures = append(f.captures, fakeCapture{Capture: c, Bodies: b})
	f.mu.Unlock()
	return nil
}

func (f *fakeSink) UpsertClientKey(_ context.Context, id, value string) error {
	f.mu.Lock()
	f.keyCalls++
	err := f.keyErr
	f.mu.Unlock()
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.keys = append(f.keys, fakeKey{ID: id, Value: value})
	f.mu.Unlock()
	return nil
}

func (f *fakeSink) Close() error {
	f.mu.Lock()
	f.closes++
	f.mu.Unlock()
	return nil
}

func (f *fakeSink) snapshot() []fakeCapture {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeCapture(nil), f.captures...)
}

func (f *fakeSink) keysSnapshot() []fakeKey {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeKey(nil), f.keys...)
}

func (f *fakeSink) closeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closes
}

func (f *fakeSink) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeSink) keyCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.keyCalls
}

func (f *fakeSink) setErr(err error) {
	f.mu.Lock()
	f.err = err
	f.mu.Unlock()
}

func (f *fakeSink) setKeyErr(err error) {
	f.mu.Lock()
	f.keyErr = err
	f.mu.Unlock()
}

// sinkSeq hands out one fresh fakeSink per Open call.
type sinkSeq struct {
	mu    sync.Mutex
	sinks []*fakeSink
	idx   int
	fail  error
}

func (s *sinkSeq) next(context.Context, storage.Config) (Sink, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail != nil {
		return nil, s.fail
	}
	var sink *fakeSink
	if s.idx < len(s.sinks) {
		sink = s.sinks[s.idx]
	} else {
		sink = &fakeSink{}
		s.sinks = append(s.sinks, sink)
	}
	s.idx++
	return sink, nil
}

func (s *sinkSeq) at(i int) *fakeSink {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sinks[i]
}

func (s *sinkSeq) all() []*fakeSink {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*fakeSink(nil), s.sinks...)
}

func (s *sinkSeq) opened() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.idx
}

func configYAML(extra string) string { return "driver: sqlite\n" + extra }

func mustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func lifecycleRaw(yaml string) []byte {
	return mustMarshal(map[string][]byte{"config_yaml": []byte(yaml)})
}

func register(t *testing.T, p *Plugin, yaml string) map[string]any {
	t.Helper()
	resp := p.Handle("plugin.register", lifecycleRaw(yaml))
	var out envelope
	if err := json.Unmarshal(resp, &out); err != nil {
		t.Fatalf("register response %q: %v", resp, err)
	}
	if !out.OK {
		t.Fatalf("register rejected valid config: %s", resp)
	}
	var result map[string]any
	if err := json.Unmarshal(out.Result, &result); err != nil {
		t.Fatalf("register result %q: %v", out.Result, err)
	}
	return result
}

func reconfigure(t *testing.T, p *Plugin, yaml string) {
	t.Helper()
	resp := p.Handle("plugin.reconfigure", lifecycleRaw(yaml))
	var out envelope
	if err := json.Unmarshal(resp, &out); err != nil {
		t.Fatalf("reconfigure response %q: %v", resp, err)
	}
	if !out.OK {
		t.Fatalf("reconfigure rejected valid config: %s", resp)
	}
}

// send marshals payload, calls the hook and asserts the transparent envelope.
func send(t *testing.T, p *Plugin, method string, payload any) {
	t.Helper()
	sendRaw(t, p, method, mustMarshal(payload))
}

func sendRaw(t *testing.T, p *Plugin, method string, raw []byte) {
	t.Helper()
	resp := p.Handle(method, raw)
	if !bytes.Equal(resp, passThrough) {
		t.Fatalf("%s must be transparent: got %s, want %s", method, resp, passThrough)
	}
}

func closePlugin(t *testing.T, p *Plugin) {
	t.Helper()
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// usageOf, pendingOf and friends expose the internal accounting so tests can
// assert that no memory or capture slot leaks across failures and reconfigures.
func usageOf(p *Plugin) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.bytes
}

func pendingOf(p *Plugin) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.pending)
}

func droppedOf(p *Plugin) uint64 { return p.dropped.Load() }

func failuresOf(p *Plugin) uint64 { return p.writeFailures.Load() }

func assertSettled(t *testing.T, p *Plugin) {
	t.Helper()
	if got := usageOf(p); got != 0 {
		t.Errorf("byte accounting leaked: %d bytes still charged after Close", got)
	}
	if got := pendingOf(p); got != 0 {
		t.Errorf("pending captures leaked: %d entries after Close", got)
	}
}

// waitFor blocks until cond holds; it fails the test after a short timeout.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// waitDrained waits until no capture or key batch is queued or in flight.
func waitDrained(t *testing.T, p *Plugin) {
	t.Helper()
	waitFor(t, "the background writer to drain", func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.bytes == 0 && len(p.pending) == 0
	})
}

func body(n int) []byte { return []byte(strings.Repeat("A", n)) }

// ---------------------------------------------------------------------------
// registration / lifecycle
// ---------------------------------------------------------------------------

func TestRegisterReturnsRegistration(t *testing.T) {
	seq := &sinkSeq{}
	p := NewWithOpener(seq.next)
	result := register(t, p, configYAML(""))
	if result["schema_version"] != float64(6) {
		t.Errorf("schema_version = %v, want 6", result["schema_version"])
	}
	meta, _ := result["metadata"].(map[string]any)
	if meta["Name"] != "cpa-distill" {
		t.Errorf("metadata.Name = %v, want cpa-distill", meta["Name"])
	}
	caps, _ := result["capabilities"].(map[string]any)
	for _, want := range []string{"request_interceptor", "request_lifecycle_plugin", "response_interceptor", "response_stream_interceptor", "websocket_response_observer"} {
		if caps[want] != true {
			t.Errorf("capability %q not advertised", want)
		}
	}
	if seq.opened() != 1 {
		t.Errorf("register opened %d backends, want 1", seq.opened())
	}
	closePlugin(t, p)
	if got := seq.at(0).closeCount(); got != 1 {
		t.Errorf("backend closed %d times, want 1", got)
	}
}

func TestRegisterRejectsInvalidConfigAndKeepsHooksTransparent(t *testing.T) {
	seq := &sinkSeq{}
	p := NewWithOpener(seq.next)
	for _, yaml := range []string{
		"driver: mysql\n",
		"driver: postgres\n", // dsn required
		"write_timeout: 0s\n",
		"write_timeout: 2m\n",
		"queue_size: 0\n",
		"queue_size: 5000\n",
		"request_ttl: -1s\n",
		"max_request_bytes: 0\n",
		"max_buffer_bytes: 1\n",
		"dsn: [\n",
	} {
		resp := p.Handle("plugin.register", lifecycleRaw(yaml))
		var out envelope
		if err := json.Unmarshal(resp, &out); err != nil {
			t.Fatalf("response %q: %v", resp, err)
		}
		if out.OK || out.Error.Code != "plugin_error" {
			t.Errorf("config %q accepted: %s", yaml, resp)
		}
	}
	if resp := p.Handle("plugin.register", []byte("{")); !bytes.Contains(resp, []byte("plugin_error")) {
		t.Errorf("malformed lifecycle request not rejected: %s", resp)
	}
	if seq.opened() != 0 {
		t.Errorf("opened %d backends for invalid configs", seq.opened())
	}
	// Without a backend hooks must still be transparent no-ops.
	for _, m := range hookMethods {
		sendRaw(t, p, m, mustMarshal(map[string]any{"RequestID": "r", "Body": []byte("AAAA")}))
	}
	assertSettled(t, p)
}

func TestRegisterReportsOpenFailureAndRecovers(t *testing.T) {
	seq := &sinkSeq{fail: errors.New("cannot connect")}
	p := NewWithOpener(seq.next)
	resp := p.Handle("plugin.register", lifecycleRaw(configYAML("")))
	if !bytes.Contains(resp, []byte("cannot open persistence database")) {
		t.Fatalf("open failure not surfaced: %s", resp)
	}
	sendRaw(t, p, "request.intercept_before", mustMarshal(map[string]any{"RequestID": "r"}))

	seq.mu.Lock()
	seq.fail = nil
	seq.mu.Unlock()
	register(t, p, configYAML(""))
	send(t, p, "request.intercept_before", map[string]any{"RequestID": "r", "Body": []byte("AAAA")})
	send(t, p, "request.complete", map[string]any{"RequestID": "r", "Outcome": "succeeded"})
	closePlugin(t, p)
	if got := len(seq.at(0).snapshot()); got != 1 {
		t.Fatalf("persisted %d captures after recovery, want 1", got)
	}
}

func TestUnknownMethodFails(t *testing.T) {
	p := NewWithOpener((&sinkSeq{}).next)
	resp := p.Handle("plugin.unknown", []byte("{}"))
	var out envelope
	if err := json.Unmarshal(resp, &out); err != nil {
		t.Fatalf("response %q: %v", resp, err)
	}
	if out.OK || out.Error.Code != "unknown_method" {
		t.Fatalf("unknown method response = %s", resp)
	}
}

// ---------------------------------------------------------------------------
// transparency
// ---------------------------------------------------------------------------

func TestHooksAreTransparentWithEmptyResult(t *testing.T) {
	seq := &sinkSeq{}
	p := NewWithOpener(seq.next)
	register(t, p, configYAML(""))
	payload := map[string]any{"RequestID": "r1", "Body": []byte("AAAA"), "Outcome": "succeeded"}
	for _, m := range hookMethods {
		send(t, p, m, payload)
		resp := p.Handle(m, mustMarshal(payload))
		var out struct {
			OK     bool           `json:"ok"`
			Result map[string]any `json:"result"`
		}
		if err := json.Unmarshal(resp, &out); err != nil {
			t.Fatalf("%s response %q: %v", m, resp, err)
		}
		if !out.OK || out.Result == nil || len(out.Result) != 0 {
			t.Errorf("%s must return ok with an empty result object, got %s", m, resp)
		}
	}
	// Lifecycle methods that do not reconfigure are transparent too.
	sendRaw(t, p, "plugin.quiesce", []byte("{}"))
	closePlugin(t, p)
}

func TestMalformedJSONIsAlwaysTransparent(t *testing.T) {
	seq := &sinkSeq{}
	p := NewWithOpener(seq.next)
	register(t, p, configYAML(""))
	payloads := [][]byte{
		nil,
		[]byte(""),
		[]byte("{"),
		[]byte("null"),
		[]byte("[]"),
		[]byte("42"),
		[]byte(`"str"`),
		[]byte("{}"),
		[]byte(`{"RequestID":123}`),
		[]byte(`{"RequestID":""}`),
		[]byte(`{"RequestID":null}`),
		[]byte(`{"RequestID":[]}`),
		[]byte(`{"RequestID":"x"`),
		[]byte("{\"RequestID\":\"x\",\"Body\":}"),
	}
	for _, m := range hookMethods {
		for i, raw := range payloads {
			resp := p.Handle(m, raw)
			if !bytes.Equal(resp, passThrough) {
				t.Errorf("%s payload #%d (%q) rewrote traffic: got %s", m, i, raw, resp)
			}
		}
	}
	closePlugin(t, p)
	if got := len(seq.at(0).snapshot()); got != 0 {
		t.Fatalf("malformed payloads produced %d captures, want 0", got)
	}
	if droppedOf(p) == 0 {
		t.Errorf("malformed payloads were not accounted as dropped")
	}
	assertSettled(t, p)
}

func TestMalformedStartDoesNotCreatePending(t *testing.T) {
	seq := &sinkSeq{}
	p := NewWithOpener(seq.next)
	register(t, p, configYAML(""))
	sendRaw(t, p, "request.intercept_before", []byte("{"))
	send(t, p, "request.intercept_after", map[string]any{"RequestID": "r"})
	send(t, p, "request.complete", map[string]any{"RequestID": "r", "Outcome": "succeeded"})
	closePlugin(t, p)
	if got := len(seq.at(0).snapshot()); got != 0 {
		t.Fatalf("a malformed start still produced %d captures", got)
	}
	assertSettled(t, p)
}

func TestEventWithoutStartIsIgnored(t *testing.T) {
	seq := &sinkSeq{}
	p := NewWithOpener(seq.next)
	register(t, p, configYAML(""))
	for _, m := range []string{"request.intercept_after", "response.intercept_after", "response.intercept_stream_chunk", "websocket.response_event"} {
		send(t, p, m, map[string]any{"RequestID": "orphan", "Body": []byte("AAAA")})
	}
	send(t, p, "request.complete", map[string]any{"RequestID": "orphan", "Outcome": "succeeded"})
	closePlugin(t, p)
	if got := len(seq.at(0).snapshot()); got != 0 {
		t.Fatalf("orphan events produced %d captures, want 0", got)
	}
	assertSettled(t, p)
}

// ---------------------------------------------------------------------------
// success-only persistence
// ---------------------------------------------------------------------------

func TestSuccessfulRequestPersistsOneCompleteCapture(t *testing.T) {
	seq := &sinkSeq{}
	p := NewWithOpener(seq.next)
	register(t, p, configYAML(""))
	const reqID = "req-ok"
	send(t, p, "request.intercept_before", map[string]any{"RequestID": reqID, "Model": "test-model", "Body": []byte("request-body")})
	send(t, p, "request.intercept_after", map[string]any{"RequestID": reqID, "Model": "upstream-model", "Body": []byte("upstream-body")})
	send(t, p, "response.intercept_after", map[string]any{"RequestID": reqID, "Body": []byte("response-body")})
	send(t, p, "request.complete", map[string]any{"RequestID": reqID, "Outcome": "succeeded"})
	closePlugin(t, p)

	captures := seq.at(0).snapshot()
	if len(captures) != 1 {
		t.Fatalf("got %d captures, want exactly 1 atomic capture", len(captures))
	}
	got := captures[0]
	if got.Capture.SessionID == "" {
		t.Error("capture has no session ID")
	}
	if got.Capture.RequestID != reqID {
		t.Errorf("capture request id = %q, want %q", got.Capture.RequestID, reqID)
	}
	if got.Capture.Model != "upstream-model" {
		t.Errorf("capture model = %q, want the last observed upstream model", got.Capture.Model)
	}
	if got.Capture.StartedAt.IsZero() || got.Capture.CompletedAt.IsZero() {
		t.Errorf("capture timestamps are incomplete: %+v", got.Capture)
	}
	// The client's original request is kept; the upstream request body is not.
	if string(got.Bodies.RequestBody) != "request-body" {
		t.Errorf("request body = %q, want the first before body", got.Bodies.RequestBody)
	}
	if string(got.Bodies.ResponseBody) != "response-body" {
		t.Errorf("response body = %q, want the non-stream response body", got.Bodies.ResponseBody)
	}
	if len(got.Bodies.WebSocketBody) != 0 {
		t.Errorf("websocket body = %q, want empty", got.Bodies.WebSocketBody)
	}
	if got.Capture.EventCount != 4 {
		t.Errorf("event count = %d, want 4 observed hooks", got.Capture.EventCount)
	}
	if got := seq.at(0).closeCount(); got != 1 {
		t.Errorf("sink closed %d times, want 1", got)
	}
	assertSettled(t, p)
}

func TestStreamChunksAreConcatenatedAndInitIsExcluded(t *testing.T) {
	seq := &sinkSeq{}
	p := NewWithOpener(seq.next)
	register(t, p, configYAML(""))
	const reqID = "req-stream"
	send(t, p, "request.intercept_before", map[string]any{"RequestID": reqID, "Stream": true, "Body": []byte("request-body")})
	// A streamed call may still emit a response.intercept_after marker; it must not
	// be appended to the chunk bytes.
	send(t, p, "response.intercept_after", map[string]any{"RequestID": reqID, "Body": []byte("MARKER")})
	send(t, p, "response.intercept_stream_chunk", map[string]any{"RequestID": reqID, "ChunkIndex": -1, "Body": []byte("INIT")})
	for i, chunk := range []string{"data: first\n\n", "data: second\n\n", "data: [DONE]\n\n"} {
		send(t, p, "response.intercept_stream_chunk", map[string]any{"RequestID": reqID, "ChunkIndex": i, "Body": []byte(chunk)})
	}
	send(t, p, "request.complete", map[string]any{"RequestID": reqID, "Outcome": "succeeded"})
	closePlugin(t, p)

	captures := seq.at(0).snapshot()
	if len(captures) != 1 {
		t.Fatalf("got %d captures, want 1", len(captures))
	}
	got := string(captures[0].Bodies.ResponseBody)
	if want := "data: first\n\ndata: second\n\ndata: [DONE]\n\n"; got != want {
		t.Errorf("stream response = %q, want %q", got, want)
	}
	if strings.Contains(got, "MARKER") || strings.Contains(got, "INIT") {
		t.Errorf("stream response contains the marker or the init frame: %q", got)
	}
	if !captures[0].Capture.Stream {
		t.Error("capture did not record the stream flag")
	}
	assertSettled(t, p)
}

func TestWebSocketBodyIsKeptSeparatelyWithNewlineBoundary(t *testing.T) {
	seq := &sinkSeq{}
	p := NewWithOpener(seq.next)
	register(t, p, configYAML(""))
	const reqID = "req-ws"
	send(t, p, "request.intercept_before", map[string]any{"RequestID": reqID, "Body": []byte("request-body")})
	send(t, p, "websocket.response_event", map[string]any{"RequestID": reqID, "Payload": []byte("frame-one")})
	send(t, p, "websocket.response_event", map[string]any{"RequestID": reqID, "Payload": []byte("frame-two")})
	send(t, p, "request.complete", map[string]any{"RequestID": reqID, "Outcome": "succeeded"})
	closePlugin(t, p)

	captures := seq.at(0).snapshot()
	if len(captures) != 1 {
		t.Fatalf("got %d captures, want 1", len(captures))
	}
	if got := string(captures[0].Bodies.WebSocketBody); got != "frame-one\nframe-two" {
		t.Errorf("websocket body = %q, want newline separated frames", got)
	}
	if len(captures[0].Bodies.ResponseBody) != 0 {
		t.Errorf("websocket payload leaked into the response body: %q", captures[0].Bodies.ResponseBody)
	}
	assertSettled(t, p)
}

func TestEmptyStreamDoesNotFallBackToTheMarker(t *testing.T) {
	seq := &sinkSeq{}
	p := NewWithOpener(seq.next)
	register(t, p, configYAML(""))
	const reqID = "req-empty-stream"
	send(t, p, "request.intercept_before", map[string]any{"RequestID": reqID, "Stream": true, "Body": []byte("request-body")})
	send(t, p, "response.intercept_after", map[string]any{"RequestID": reqID, "Body": []byte("MARKER")})
	send(t, p, "response.intercept_stream_chunk", map[string]any{"RequestID": reqID, "ChunkIndex": -1, "Body": nil})
	send(t, p, "request.complete", map[string]any{"RequestID": reqID, "Outcome": "succeeded"})
	closePlugin(t, p)

	captures := seq.at(0).snapshot()
	if len(captures) != 1 {
		t.Fatalf("got %d captures, want 1", len(captures))
	}
	// An observed stream that produced no content must stay empty: falling back to
	// the response.intercept_after marker would present a marker as the response.
	if got := captures[0].Bodies.ResponseBody; len(got) != 0 {
		t.Errorf("empty stream response = %q, want empty", got)
	}
	assertSettled(t, p)
}

func TestMalformedBodyDropsWholeCapture(t *testing.T) {
	for _, tc := range []struct {
		name    string
		method  string
		payload map[string]any
	}{
		{"invalid base64", "response.intercept_after", map[string]any{"RequestID": "r", "Body": "!!!"}},
		{"body is a number", "response.intercept_after", map[string]any{"RequestID": "r", "Body": 42}},
		{"chunk index is text", "response.intercept_stream_chunk", map[string]any{"RequestID": "r", "ChunkIndex": "zero", "Body": []byte("x")}},
		{"payload is not base64", "websocket.response_event", map[string]any{"RequestID": "r", "Payload": "***"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seq := &sinkSeq{}
			p := NewWithOpener(seq.next)
			register(t, p, configYAML(""))
			send(t, p, "request.intercept_before", map[string]any{"RequestID": "r", "Body": []byte("request-body")})
			send(t, p, tc.method, tc.payload)
			send(t, p, "request.complete", map[string]any{"RequestID": "r", "Outcome": "succeeded"})
			closePlugin(t, p)
			if got := len(seq.at(0).snapshot()); got != 0 {
				t.Fatalf("a malformed body still stored %d captures; a partial body must never be presented as complete", got)
			}
			if droppedOf(p) == 0 {
				t.Error("the malformed capture was not accounted as dropped")
			}
			assertSettled(t, p)
		})
	}
}

func TestOnlySuccessfulOutcomesPersist(t *testing.T) {
	for _, tc := range []struct {
		outcome string
		persist bool
	}{
		{"succeeded", true},
		{"failed", false},
		{"canceled", false},
		{"error", false},
		{"", false},
	} {
		t.Run(tc.outcome, func(t *testing.T) {
			seq := &sinkSeq{}
			p := NewWithOpener(seq.next)
			register(t, p, configYAML(""))
			send(t, p, "request.intercept_before", map[string]any{"RequestID": "r", "Body": []byte("AAAA")})
			send(t, p, "request.intercept_after", map[string]any{"RequestID": "r", "Body": []byte("BBBB")})
			send(t, p, "request.complete", map[string]any{"RequestID": "r", "Outcome": tc.outcome})
			closePlugin(t, p)

			captures := seq.at(0).snapshot()
			if tc.persist {
				if len(captures) != 1 {
					t.Fatalf("succeeded request persisted %d captures, want 1", len(captures))
				}
				return
			}
			if len(captures) != 0 {
				t.Fatalf("outcome %q persisted %d captures, want 0", tc.outcome, len(captures))
			}
			assertSettled(t, p)
		})
	}
}

func TestUnfinishedRequestIsDiscardedOnClose(t *testing.T) {
	seq := &sinkSeq{}
	p := NewWithOpener(seq.next)
	register(t, p, configYAML(""))
	send(t, p, "request.intercept_before", map[string]any{"RequestID": "r", "Body": []byte("AAAA")})
	send(t, p, "response.intercept_stream_chunk", map[string]any{"RequestID": "r", "ChunkIndex": 0, "Body": []byte("BBBB")})
	closePlugin(t, p)
	if got := len(seq.at(0).snapshot()); got != 0 {
		t.Fatalf("unfinished request persisted %d captures, want 0", got)
	}
	assertSettled(t, p)
}

// TestSmokeParity mirrors scripts/smoke.py: four requests, two successful.
func TestSmokeParity(t *testing.T) {
	seq := &sinkSeq{}
	p := NewWithOpener(seq.next)
	register(t, p, configYAML(""))
	requests := []struct {
		id      string
		stream  bool
		outcome string
	}{
		{"ok", false, "succeeded"},
		{"stream", true, "succeeded"},
		{"failed", false, "failed"},
		{"cancel", true, "canceled"},
	}
	with := func(base map[string]any, extra map[string]any) map[string]any {
		out := map[string]any{}
		for k, v := range base {
			out[k] = v
		}
		for k, v := range extra {
			out[k] = v
		}
		return out
	}
	for _, r := range requests {
		common := map[string]any{"RequestID": r.id, "Model": "test-model", "Stream": r.stream}
		send(t, p, "request.intercept_before", with(common, map[string]any{
			"Body":     []byte(`{"messages":[{"role":"user","content":"hello"}]}`),
			"Headers":  map[string]any{"Authorization": []string{"Bearer never-store"}},
			"Metadata": map[string]any{"api_key": "never-store"},
		}))
		send(t, p, "request.intercept_after", with(common, map[string]any{
			"Body": []byte("upstream request"),
		}))
		if r.stream {
			send(t, p, "response.intercept_stream_chunk", with(common, map[string]any{"ChunkIndex": -1, "Body": nil}))
			for i, chunk := range []string{"data: first\n\n", "data: second\n\n", "data: [DONE]\n\n"} {
				send(t, p, "response.intercept_stream_chunk", with(common, map[string]any{
					"ChunkIndex": i,
					"Body":       []byte(chunk),
				}))
			}
		} else if r.outcome == "succeeded" {
			send(t, p, "response.intercept_after", with(common, map[string]any{
				"Body": []byte("answer\x00\xff"),
			}))
		}
		send(t, p, "request.complete", with(common, map[string]any{"Outcome": r.outcome}))
	}
	// A response hook without a request ID must pass through and store nothing.
	sendRaw(t, p, "response.intercept_after", []byte("{}"))
	closePlugin(t, p)

	byRequest := map[string]fakeCapture{}
	for _, c := range seq.at(0).snapshot() {
		byRequest[c.Capture.RequestID] = c
	}
	if len(byRequest) != 2 {
		t.Fatalf("persisted captures = %v, want only ok and stream", byRequest)
	}
	ok := byRequest["ok"]
	if string(ok.Bodies.RequestBody) != `{"messages":[{"role":"user","content":"hello"}]}` {
		t.Errorf("non-stream request body changed: %q", ok.Bodies.RequestBody)
	}
	if !bytes.Equal(ok.Bodies.ResponseBody, []byte("answer\x00\xff")) {
		t.Errorf("binary response body changed: %q", ok.Bodies.ResponseBody)
	}
	stream := byRequest["stream"]
	if string(stream.Bodies.RequestBody) != `{"messages":[{"role":"user","content":"hello"}]}` {
		t.Errorf("stream request body changed: %q", stream.Bodies.RequestBody)
	}
	if got := string(stream.Bodies.ResponseBody); got != "data: first\n\ndata: second\n\ndata: [DONE]\n\n" {
		t.Errorf("stream chunks did not round-trip: %q", got)
	}
	// Credentials must never reach any stored byte.
	for id, c := range byRequest {
		for name, raw := range map[string][]byte{
			"request": c.Bodies.RequestBody, "response": c.Bodies.ResponseBody, "websocket": c.Bodies.WebSocketBody,
		} {
			if strings.Contains(string(raw), "never-store") {
				t.Errorf("%s %s body leaked credentials: %q", id, name, raw)
			}
		}
	}
	assertSettled(t, p)
}

// ---------------------------------------------------------------------------
// credential stripping
// ---------------------------------------------------------------------------

func TestHeadersAndMetadataAreStrippedBodyIsExact(t *testing.T) {
	seq := &sinkSeq{}
	p := NewWithOpener(seq.next)
	register(t, p, configYAML(""))
	binary := []byte{0x00, 0xff, 0x10, '<', '&', '>', 0x7f, 'a'}
	send(t, p, "request.intercept_before", map[string]any{
		"RequestID":        "r",
		"Model":            "test-model",
		"Body":             binary,
		"Headers":          map[string]any{"Authorization": "Bearer never-store"},
		"RequestHeaders":   map[string]any{"Cookie": "never-store"},
		"ResponseHeaders":  map[string]any{"Set-Cookie": "never-store"},
		"Metadata":         map[string]any{"api_key": "never-store"},
		"host_callback_id": "never-store",
	})
	send(t, p, "request.complete", map[string]any{"RequestID": "r", "Outcome": "succeeded"})
	closePlugin(t, p)

	captures := seq.at(0).snapshot()
	if len(captures) != 1 {
		t.Fatalf("persisted %d captures, want 1", len(captures))
	}
	if !bytes.Equal(captures[0].Bodies.RequestBody, binary) {
		t.Errorf("binary body changed: %q", captures[0].Bodies.RequestBody)
	}
	if captures[0].Capture.Model != "test-model" {
		t.Errorf("non-sensitive field Model was dropped: %+v", captures[0].Capture)
	}
	// Only the body bytes and typed attributes exist now: credential-bearing hook
	// fields (Headers, Metadata, ...) have no storage location at all.
	for name, raw := range map[string][]byte{
		"request": captures[0].Bodies.RequestBody, "response": captures[0].Bodies.ResponseBody, "websocket": captures[0].Bodies.WebSocketBody,
	} {
		if strings.Contains(string(raw), "never-store") {
			t.Errorf("%s body leaked credentials: %q", name, raw)
		}
	}
	assertSettled(t, p)
}

// ---------------------------------------------------------------------------
// limits: the whole request is skipped, never a prefix
// ---------------------------------------------------------------------------

func TestSingleOversizedEventSkipsWholeRequest(t *testing.T) {
	seq := &sinkSeq{}
	p := NewWithOpener(seq.next)
	register(t, p, configYAML("max_request_bytes: 2048\nmax_buffer_bytes: 8192\n"))
	send(t, p, "request.intercept_before", map[string]any{"RequestID": "big", "Body": body(4000)})
	send(t, p, "request.intercept_after", map[string]any{"RequestID": "big", "Body": body(10)})
	send(t, p, "request.complete", map[string]any{"RequestID": "big", "Outcome": "succeeded"})
	closePlugin(t, p)
	if got := len(seq.at(0).snapshot()); got != 0 {
		t.Fatalf("oversized request persisted %d captures, want 0 (no partial capture)", got)
	}
	if droppedOf(p) == 0 {
		t.Errorf("oversized request was not accounted as dropped")
	}
	assertSettled(t, p)
}

func TestRequestOverflowMidTraceSkipsWholeRequest(t *testing.T) {
	seq := &sinkSeq{}
	p := NewWithOpener(seq.next)
	register(t, p, configYAML("max_request_bytes: 4096\nmax_buffer_bytes: 16384\n"))
	send(t, p, "request.intercept_before", map[string]any{"RequestID": "r", "Body": body(1000)})
	send(t, p, "response.intercept_after", map[string]any{"RequestID": "r", "Body": body(4000)})
	send(t, p, "request.complete", map[string]any{"RequestID": "r", "Outcome": "succeeded"})
	closePlugin(t, p)
	if got := len(seq.at(0).snapshot()); got != 0 {
		t.Fatalf("request that overflowed mid-trace persisted %d captures, want 0", got)
	}
	assertSettled(t, p)
}

// The per-request capacity boundary is exact: a capture that fits its budget byte
// for byte is kept, one byte more is skipped whole. A raw-JSON-length estimate
// (about a third larger than the decoded body) would drop the fitting capture.
func TestCapacityBoundaryIsExact(t *testing.T) {
	const limit = 1257 // 1000 body bytes + 1 request id byte + 256 overhead
	seq := &sinkSeq{}
	p := NewWithOpener(seq.next)
	register(t, p, configYAML(fmt.Sprintf("max_request_bytes: %d\nmax_buffer_bytes: %d\n", limit, limit)))
	send(t, p, "request.intercept_before", map[string]any{"RequestID": "r", "Body": body(1000)})
	send(t, p, "request.complete", map[string]any{"RequestID": "r", "Outcome": "succeeded"})
	send(t, p, "request.intercept_before", map[string]any{"RequestID": "r", "Body": body(1001)})
	send(t, p, "request.complete", map[string]any{"RequestID": "r", "Outcome": "succeeded"})
	closePlugin(t, p)

	captures := seq.at(0).snapshot()
	if len(captures) != 1 {
		t.Fatalf("persisted %d captures, want 1 (the exactly fitting one)", len(captures))
	}
	if got := len(captures[0].Bodies.RequestBody); got != 1000 {
		t.Errorf("stored request body = %d bytes, want 1000", got)
	}
	assertSettled(t, p)
}

// An escaped base64 body has no cheap length estimate, so it must be accepted
// whenever its decoded bytes fit: the pre-decode check may not reject it on the
// longer raw JSON length.
func TestEscapedBodyIsNotRejectedBeforeDecoding(t *testing.T) {
	const limit = 1257 // 1000 body bytes + 1 request id byte + 256 overhead
	escaped := func(n int) json.RawMessage {
		encoded := base64.StdEncoding.EncodeToString(body(n))
		return json.RawMessage(`"` + strings.ReplaceAll(encoded, "=", `\u003d`) + `"`)
	}
	seq := &sinkSeq{}
	p := NewWithOpener(seq.next)
	register(t, p, configYAML(fmt.Sprintf("max_request_bytes: %d\nmax_buffer_bytes: %d\n", limit, limit)))
	send(t, p, "request.intercept_before", map[string]any{"RequestID": "r", "Body": escaped(1000)})
	send(t, p, "request.complete", map[string]any{"RequestID": "r", "Outcome": "succeeded"})
	send(t, p, "request.intercept_before", map[string]any{"RequestID": "r", "Body": escaped(1001)})
	send(t, p, "request.complete", map[string]any{"RequestID": "r", "Outcome": "succeeded"})
	closePlugin(t, p)

	captures := seq.at(0).snapshot()
	if len(captures) != 1 {
		t.Fatalf("persisted %d captures, want 1 (the exactly fitting escaped one)", len(captures))
	}
	if got := len(captures[0].Bodies.RequestBody); got != 1000 {
		t.Errorf("stored request body = %d bytes, want 1000", got)
	}
	assertSettled(t, p)
}

func TestGlobalBufferLimitSkipsOnlyTheOffendingRequest(t *testing.T) {
	seq := &sinkSeq{}
	p := NewWithOpener(seq.next)
	register(t, p, configYAML("max_request_bytes: 3072\nmax_buffer_bytes: 3072\n"))
	// r1 occupies most of the buffer and must survive untouched.
	send(t, p, "request.intercept_before", map[string]any{"RequestID": "r1", "Body": body(1000)})
	// r2 does not fit in the remaining budget and is skipped whole.
	send(t, p, "request.intercept_before", map[string]any{"RequestID": "r2", "Body": body(2000)})
	send(t, p, "request.intercept_after", map[string]any{"RequestID": "r2", "Body": body(10)})
	send(t, p, "request.complete", map[string]any{"RequestID": "r2", "Outcome": "succeeded"})
	send(t, p, "request.complete", map[string]any{"RequestID": "r1", "Outcome": "succeeded"})
	closePlugin(t, p)

	captures := seq.at(0).snapshot()
	if len(captures) != 1 {
		t.Fatalf("persisted %d captures, want 1 (r1 only)", len(captures))
	}
	if captures[0].Capture.RequestID != "r1" {
		t.Errorf("unexpected request %q persisted", captures[0].Capture.RequestID)
	}
	assertSettled(t, p)
}

func TestQueueFullSkipsWholeRequest(t *testing.T) {
	sink := &fakeSink{gate: make(chan struct{}), entered: make(chan struct{}, 1)}
	seq := &sinkSeq{sinks: []*fakeSink{sink}}
	p := NewWithOpener(seq.next)
	register(t, p, configYAML("queue_size: 1\n"))

	// r1 occupies the writer; it parks inside AppendCapture.
	send(t, p, "request.intercept_before", map[string]any{"RequestID": "r1", "Body": []byte("AAAA")})
	send(t, p, "request.complete", map[string]any{"RequestID": "r1", "Outcome": "succeeded"})
	select {
	case <-sink.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("background writer never reached the sink")
	}
	// r2 fills the single queue slot.
	send(t, p, "request.intercept_before", map[string]any{"RequestID": "r2", "Body": []byte("AAAA")})
	send(t, p, "request.complete", map[string]any{"RequestID": "r2", "Outcome": "succeeded"})
	// r3 finds the queue full and must be dropped in full.
	send(t, p, "request.intercept_before", map[string]any{"RequestID": "r3", "Body": []byte("AAAA")})
	send(t, p, "request.intercept_after", map[string]any{"RequestID": "r3", "Body": []byte("BBBB")})
	send(t, p, "request.complete", map[string]any{"RequestID": "r3", "Outcome": "succeeded"})

	close(sink.gate)
	closePlugin(t, p)
	seen := map[string]bool{}
	for _, c := range sink.snapshot() {
		seen[c.Capture.RequestID] = true
	}
	if seen["r3"] {
		t.Fatal("request dropped by a full queue was still persisted")
	}
	if got := len(sink.snapshot()); got != 2 {
		t.Errorf("persisted %d captures, want 2 (r1 + r2)", got)
	}
	assertSettled(t, p)
}

func TestMaxPendingRequestsLimit(t *testing.T) {
	seq := &sinkSeq{}
	p := NewWithOpener(seq.next)
	register(t, p, configYAML("max_pending_requests: 2\n"))
	for _, id := range []string{"r1", "r2", "r3"} {
		send(t, p, "request.intercept_before", map[string]any{"RequestID": id, "Body": []byte("AAAA")})
	}
	for _, id := range []string{"r1", "r2", "r3"} {
		send(t, p, "request.complete", map[string]any{"RequestID": id, "Outcome": "succeeded"})
	}
	closePlugin(t, p)
	seen := map[string]bool{}
	for _, c := range seq.at(0).snapshot() {
		seen[c.Capture.RequestID] = true
	}
	if seen["r3"] {
		t.Fatalf("request above max_pending_requests was persisted")
	}
	if !seen["r1"] || !seen["r2"] {
		t.Fatalf("admitted requests were lost: %v", seen)
	}
	assertSettled(t, p)
}

func TestExpiredPendingIsDropped(t *testing.T) {
	seq := &sinkSeq{}
	p := NewWithOpener(seq.next)
	register(t, p, configYAML("request_ttl: 1ms\n"))
	send(t, p, "request.intercept_before", map[string]any{"RequestID": "stale", "Body": []byte("AAAA")})

	// Force the next hook to run the periodic sweep.
	p.mu.Lock()
	p.lastSweep = time.Now().Add(-2 * time.Minute)
	p.mu.Unlock()
	time.Sleep(5 * time.Millisecond)

	send(t, p, "request.intercept_before", map[string]any{"RequestID": "fresh", "Body": []byte("AAAA")})
	send(t, p, "request.complete", map[string]any{"RequestID": "stale", "Outcome": "succeeded"})
	send(t, p, "request.complete", map[string]any{"RequestID": "fresh", "Outcome": "succeeded"})
	closePlugin(t, p)

	captures := seq.at(0).snapshot()
	if len(captures) != 1 {
		t.Fatalf("persisted %d captures, want 1 (fresh only)", len(captures))
	}
	if captures[0].Capture.RequestID != "fresh" {
		t.Errorf("expired capture %q was persisted", captures[0].Capture.RequestID)
	}
	assertSettled(t, p)
}

// ---------------------------------------------------------------------------
// the hook path never waits for the database
// ---------------------------------------------------------------------------

func TestBlockingDatabaseDoesNotBlockHooks(t *testing.T) {
	sink := &fakeSink{gate: make(chan struct{}), entered: make(chan struct{}, 1)}
	seq := &sinkSeq{sinks: []*fakeSink{sink}}
	p := NewWithOpener(seq.next)
	register(t, p, configYAML("queue_size: 2\n"))

	send(t, p, "request.intercept_before", map[string]any{"RequestID": "parked", "Body": []byte("AAAA")})
	send(t, p, "request.complete", map[string]any{"RequestID": "parked", "Outcome": "succeeded"})
	select {
	case <-sink.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("background writer never reached the sink")
	}

	// The writer is parked inside AppendCapture; every further hook must return at
	// once even though its capture can no longer be written.
	start := time.Now()
	for i := 0; i < 500; i++ {
		id := fmt.Sprintf("live-%d", i)
		send(t, p, "request.intercept_before", map[string]any{"RequestID": id, "Body": []byte("AAAA")})
		send(t, p, "request.complete", map[string]any{"RequestID": id, "Outcome": "succeeded"})
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("hooks stalled behind a blocked database write: %v for 1000 calls", elapsed)
	}

	close(sink.gate)
	closePlugin(t, p)
	assertSettled(t, p)
}

func TestFailingDatabaseDoesNotBlockHooksAndPersistsNothing(t *testing.T) {
	sink := &fakeSink{}
	sink.setErr(errors.New("database is down"))
	seq := &sinkSeq{sinks: []*fakeSink{sink}}
	p := NewWithOpener(seq.next)
	register(t, p, configYAML(""))

	start := time.Now()
	for i := 0; i < 200; i++ {
		id := fmt.Sprintf("r-%d", i)
		send(t, p, "request.intercept_before", map[string]any{"RequestID": id, "Body": []byte("AAAA")})
		send(t, p, "request.complete", map[string]any{"RequestID": id, "Outcome": "succeeded"})
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("hooks stalled behind a failing database: %v", elapsed)
	}
	closePlugin(t, p)
	if got := len(sink.snapshot()); got != 0 {
		t.Fatalf("a failing sink recorded %d captures", got)
	}
	if failuresOf(p) == 0 {
		t.Errorf("write failures were not accounted")
	}
	assertSettled(t, p)
}

// ---------------------------------------------------------------------------
// concurrency
// ---------------------------------------------------------------------------

func TestConcurrentRequestIDsAreIsolated(t *testing.T) {
	seq := &sinkSeq{}
	p := NewWithOpener(seq.next)
	register(t, p, configYAML("max_pending_requests: 1024\nqueue_size: 128\n"))
	const workers = 64
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			id := fmt.Sprintf("req-%03d", n)
			for _, step := range []struct {
				method  string
				payload map[string]any
			}{
				{"request.intercept_before", map[string]any{"RequestID": id, "Body": []byte(id)}},
				{"request.intercept_after", map[string]any{"RequestID": id, "Body": []byte(id)}},
				{"response.intercept_after", map[string]any{"RequestID": id, "Body": []byte(id)}},
				{"request.complete", map[string]any{"RequestID": id, "Outcome": "succeeded"}},
			} {
				resp := p.Handle(step.method, mustMarshal(step.payload))
				if !bytes.Equal(resp, passThrough) {
					t.Errorf("%s from %s was not transparent: %s", step.method, id, resp)
				}
			}
		}(i)
	}
	wg.Wait()
	closePlugin(t, p)

	byRequest := map[string]fakeCapture{}
	for _, c := range seq.at(0).snapshot() {
		byRequest[c.Capture.RequestID] = c
	}
	if len(byRequest) != workers {
		t.Fatalf("persisted %d distinct requests, want %d", len(byRequest), workers)
	}
	for id, c := range byRequest {
		if string(c.Bodies.RequestBody) != id {
			t.Errorf("%s request body = %q, want %q", id, c.Bodies.RequestBody, id)
		}
		if string(c.Bodies.ResponseBody) != id {
			t.Errorf("%s response body = %q, want %q", id, c.Bodies.ResponseBody, id)
		}
		if c.Capture.EventCount != 4 {
			t.Errorf("%s event count = %d, want 4", id, c.Capture.EventCount)
		}
	}
	assertSettled(t, p)
}

func TestCloseAndReconfigureRaceHasNoLeakOrPanic(t *testing.T) {
	seq := &sinkSeq{}
	p := NewWithOpener(seq.next)
	register(t, p, configYAML(""))

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				id := fmt.Sprintf("race-%d-%d", worker, n)
				p.Handle("request.intercept_before", mustMarshal(map[string]any{"RequestID": id, "Body": []byte("AAAA")}))
				p.Handle("response.intercept_stream_chunk", mustMarshal(map[string]any{"RequestID": id, "ChunkIndex": 0, "Body": []byte("BBBB")}))
				p.Handle("request.complete", mustMarshal(map[string]any{"RequestID": id, "Outcome": "succeeded"}))
			}
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 40; i++ {
			p.Handle("plugin.reconfigure", lifecycleRaw(configYAML(fmt.Sprintf("queue_size: %d\n", 4+i%3))))
		}
	}()
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
	closePlugin(t, p)

	for i, s := range seq.all() {
		if got := s.closeCount(); got != 1 {
			t.Errorf("backend %d closed %d times, want exactly 1", i, got)
		}
	}
	assertSettled(t, p)
}

func TestShutdownIsIdempotentAndStopsRecording(t *testing.T) {
	seq := &sinkSeq{}
	p := NewWithOpener(seq.next)
	register(t, p, configYAML(""))
	send(t, p, "request.intercept_before", map[string]any{"RequestID": "r", "Body": []byte("AAAA")})
	send(t, p, "request.complete", map[string]any{"RequestID": "r", "Outcome": "succeeded"})
	sendRaw(t, p, "plugin.shutdown", []byte("{}"))
	if got := seq.at(0).closeCount(); got != 1 {
		t.Fatalf("shutdown closed the backend %d times, want 1", got)
	}
	if got := len(seq.at(0).snapshot()); got != 1 {
		t.Fatalf("shutdown drained %d captures, want 1", got)
	}
	// Hooks after shutdown stay transparent and record nothing.
	send(t, p, "request.intercept_before", map[string]any{"RequestID": "after", "Body": []byte("AAAA")})
	send(t, p, "request.complete", map[string]any{"RequestID": "after", "Outcome": "succeeded"})
	sendRaw(t, p, "plugin.shutdown", []byte("{}"))
	if got := seq.at(0).closeCount(); got != 1 {
		t.Errorf("second shutdown closed the backend again (%d)", got)
	}
	if got := len(seq.at(0).snapshot()); got != 1 {
		t.Errorf("hooks after shutdown persisted %d extra captures", got-1)
	}
	assertSettled(t, p)
}

// ---------------------------------------------------------------------------
// reconfiguration
// ---------------------------------------------------------------------------

func TestReconfigureDrainsOldQueueAndSwitchesBackend(t *testing.T) {
	seq := &sinkSeq{}
	p := NewWithOpener(seq.next)
	register(t, p, configYAML(""))

	send(t, p, "request.intercept_before", map[string]any{"RequestID": "old", "Body": []byte("AAAA")})
	send(t, p, "request.complete", map[string]any{"RequestID": "old", "Outcome": "succeeded"})
	reconfigure(t, p, configYAML("queue_size: 8\n"))

	if seq.opened() != 2 {
		t.Fatalf("reconfigure opened %d backends, want 2", seq.opened())
	}
	if got := seq.at(0).closeCount(); got != 1 {
		t.Fatalf("old backend closed %d times, want 1", got)
	}
	if got := len(seq.at(0).snapshot()); got != 1 {
		t.Fatalf("old backend drained %d captures, want 1", got)
	}

	send(t, p, "request.intercept_before", map[string]any{"RequestID": "new", "Body": []byte("AAAA")})
	send(t, p, "request.complete", map[string]any{"RequestID": "new", "Outcome": "succeeded"})
	closePlugin(t, p)
	if got := len(seq.at(1).snapshot()); got != 1 {
		t.Fatalf("new backend captured %d captures, want 1", got)
	}
	for _, c := range seq.at(1).snapshot() {
		if c.Capture.RequestID != "new" {
			t.Errorf("new backend received stale request %q", c.Capture.RequestID)
		}
	}
	assertSettled(t, p)
}

// A stale capture must never be aborted against a newer backend that merely
// shares its request ID: the guard checks both the backend and the entry pointer.
func TestAbortCaptureNeverDropsANewerBackendsCapture(t *testing.T) {
	seq := &sinkSeq{}
	p := NewWithOpener(seq.next)
	register(t, p, configYAML(""))
	send(t, p, "request.intercept_before", map[string]any{"RequestID": "r", "Body": []byte("AAAA")})
	p.mu.Lock()
	oldBackend := p.current
	stale := p.pending["r"]
	p.mu.Unlock()
	if stale == nil {
		t.Fatal("no pending capture to abort")
	}

	reconfigure(t, p, configYAML("queue_size: 8\n"))
	send(t, p, "request.intercept_before", map[string]any{"RequestID": "r", "Body": []byte("BBBB")})
	p.mu.Lock()
	current := p.pending["r"]
	p.mu.Unlock()
	if current == nil || current == stale {
		t.Fatal("the new backend did not create its own capture")
	}

	p.abortCapture(oldBackend, stale, "r")
	p.mu.Lock()
	got := p.pending["r"]
	p.mu.Unlock()
	if got != current {
		t.Fatal("aborting a stale capture dropped the new backend's capture")
	}

	send(t, p, "request.complete", map[string]any{"RequestID": "r", "Outcome": "succeeded"})
	closePlugin(t, p)
	if got := len(seq.at(1).snapshot()); got != 1 {
		t.Fatalf("new backend captured %d captures, want 1", got)
	}
	assertSettled(t, p)
}

func TestReconfigureWithSameConfigKeepsBackend(t *testing.T) {
	seq := &sinkSeq{}
	p := NewWithOpener(seq.next)
	register(t, p, configYAML(""))
	reconfigure(t, p, configYAML(""))
	if seq.opened() != 1 {
		t.Fatalf("identical reconfigure reopened the backend (%d opens)", seq.opened())
	}
	if got := seq.at(0).closeCount(); got != 0 {
		t.Fatalf("identical reconfigure closed the live backend")
	}
	send(t, p, "request.intercept_before", map[string]any{"RequestID": "r", "Body": []byte("AAAA")})
	send(t, p, "request.complete", map[string]any{"RequestID": "r", "Outcome": "succeeded"})
	closePlugin(t, p)
	if got := len(seq.at(0).snapshot()); got != 1 {
		t.Fatalf("live backend captured %d captures, want 1", got)
	}
	assertSettled(t, p)
}

// One total drain budget cancels the active write and skips the remaining queue.
// The per-batch timeout must not multiply by queue depth during unload/reconfigure.
func TestReconfigureDrainTerminates(t *testing.T) {
	sink := &fakeSink{gate: make(chan struct{}), entered: make(chan struct{}, 1)}
	seq := &sinkSeq{sinks: []*fakeSink{sink, {}}}
	p := NewWithOpener(seq.next)
	t.Cleanup(func() { _ = p.Close() })
	cfg := configYAML("write_timeout: 30s\ndrain_timeout: 20ms\nqueue_size: 3\n")
	register(t, p, cfg)
	for i := 0; i < 4; i++ {
		id := fmt.Sprintf("hung-%d", i)
		send(t, p, "request.intercept_before", map[string]any{"RequestID": id, "Body": []byte("AAAA")})
		send(t, p, "request.complete", map[string]any{"RequestID": id, "Outcome": "succeeded"})
		if i == 0 {
			select {
			case <-sink.entered:
			case <-time.After(2 * time.Second):
				t.Fatal("writer did not start")
			}
		}
	}
	start := time.Now()
	reconfigure(t, p, cfg+"request_ttl: 31m\n")
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("total drain budget was not enforced: %v", elapsed)
	}
	if calls := sink.callCount(); calls != 1 {
		t.Fatalf("started %d writes; queued tail should be discarded after cancellation", calls)
	}
	if sink.closeCount() != 1 {
		t.Fatal("old writer must stop and close before returning")
	}
	closePlugin(t, p)
	assertSettled(t, p)
}

// ---------------------------------------------------------------------------
// client keys
// ---------------------------------------------------------------------------

// A failed key upsert must be retried on the next usage of the same key, and a
// successful one must not be enqueued again.
func TestUsageKeyIsRetriedAfterFailureThenDeduplicated(t *testing.T) {
	sink := &fakeSink{}
	sink.setKeyErr(errors.New("database is down"))
	seq := &sinkSeq{sinks: []*fakeSink{sink}}
	p := NewWithOpener(seq.next)
	register(t, p, configYAML(""))
	t.Cleanup(func() { _ = p.Close() })

	key := "sk-client-retry"
	send(t, p, "usage.handle", map[string]any{"APIKey": key})
	waitFor(t, "the failed key write", func() bool { return sink.keyCallCount() == 1 })
	waitDrained(t, p)
	if got := len(sink.keysSnapshot()); got != 0 {
		t.Fatalf("a failed upsert stored %d keys", got)
	}

	sink.setKeyErr(nil)
	send(t, p, "usage.handle", map[string]any{"APIKey": key})
	waitFor(t, "the retried key write", func() bool { return len(sink.keysSnapshot()) == 1 })
	waitDrained(t, p)

	// The same key is now known: further usage must not enqueue another upsert.
	before := sink.keyCallCount()
	send(t, p, "usage.handle", map[string]any{"APIKey": key})
	time.Sleep(20 * time.Millisecond)
	if got := sink.keyCallCount(); got != before {
		t.Fatalf("a stored key was upserted again (%d -> %d)", before, got)
	}
	waitDrained(t, p)
	assertSettled(t, p)
}

// A full queue must not mark a key as stored: the next usage retries it.
func TestUsageKeyIsNotLostWhenTheQueueIsFull(t *testing.T) {
	sink := &fakeSink{gate: make(chan struct{}), entered: make(chan struct{}, 1)}
	seq := &sinkSeq{sinks: []*fakeSink{sink}}
	p := NewWithOpener(seq.next)
	register(t, p, configYAML("queue_size: 1\n"))
	t.Cleanup(func() { _ = p.Close() })

	// Park the writer on a capture and fill the single queue slot.
	send(t, p, "request.intercept_before", map[string]any{"RequestID": "park", "Body": []byte("AAAA")})
	send(t, p, "request.complete", map[string]any{"RequestID": "park", "Outcome": "succeeded"})
	select {
	case <-sink.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("writer did not start")
	}
	send(t, p, "request.intercept_before", map[string]any{"RequestID": "queued", "Body": []byte("AAAA")})
	send(t, p, "request.complete", map[string]any{"RequestID": "queued", "Outcome": "succeeded"})

	// The queue is full: this usage cannot be enqueued and must not be remembered.
	send(t, p, "usage.handle", map[string]any{"APIKey": "sk-client-full"})
	if got := sink.keyCallCount(); got != 0 {
		t.Fatalf("a usage was written while the writer was parked (%d)", got)
	}

	close(sink.gate)
	waitDrained(t, p)
	send(t, p, "usage.handle", map[string]any{"APIKey": "sk-client-full"})
	waitFor(t, "the retried key write", func() bool { return len(sink.keysSnapshot()) == 1 })
	waitDrained(t, p)
	assertSettled(t, p)
}
