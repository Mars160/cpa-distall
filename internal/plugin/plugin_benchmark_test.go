package plugin

// End-to-end performance baseline for the hook path plus the real SQLite sink.
//
// The benchmark drives Plugin.Handle exactly like the host does, using the real
// hook envelopes (body carried as a base64 string inside the hook JSON) and
// persists through storage.Open on a real temporary SQLite file (WAL), never
// through the fake sink used by the unit tests. Every round is a fixed logical
// load:
//
//	requests   = 32 per round, spread over 1, 2, 4 or 8 concurrent callers
//	plain      = 4 hooks  (before, after, response-after, complete)
//	stream     = 12 hooks (before, after, 24 B marker, 8 chunks, complete)
//	body       = 2 KiB per request hook, 24 KiB response body per request,
//	             identical in both shapes (see benchExpectedBodies)
//
// Envelope contract: response.intercept_stream_chunk carries its bytes in Body
// with ChunkIndex 0, exactly like the host (the older baseline carried them in a
// non-contract Chunk field; the call sites keep the "Chunk" marker so the
// envelope stays byte-for-byte comparable with the corrected baseline). The
// stream shape's response.intercept_after is a 24 B marker and is NOT part of the
// stored response: the stored response is the concatenation of the 8 chunk
// bodies. Every envelope is built before the timer starts, so the measured window
// contains only Plugin.Handle and the background writer.
//
// Stored bytes changed meaning with the raw-body storage layout: the plugin no
// longer writes hook envelopes (no Metadata/Headers, no per-event JSON), it writes
// the decoded request body plus the response body. stored-B/req is therefore the
// sum of those two raw bodies, and sent-B/req is still the full host envelope.
// The two numbers are not comparable with the old event-payload metric.
//
// Because the writer is asynchronous, hook speed and persistence throughput are
// different numbers and both are reported:
//
//	hook-ns/req      time spent inside Plugin.Handle (what the proxy pays)
//	drain-ns/req     time until the background writer has stored the round
//	persisted-req/s  requests per second measured over hook + drain together
//	stored-B/req     raw request+response bytes actually stored per request
//	sent-B/req       hook envelope bytes the host sends per request
//
// The queue and buffer limits are deliberately raised for this benchmark so no
// request can be skipped for capacity reasons. That is asserted, not assumed:
// every round fails if any drop or write failure was counted, and the persisted
// rows are counted back (one capture per request, the exact hook count, and the
// exact body bytes). A throughput number therefore can never be inflated by work
// the plugin silently discarded. Production defaults (queue_size 32) would drop
// under this load, which is exactly the effect the assertion prevents here.
//
// BenchmarkPluginHookOverhead measures the same hook load against a no-op sink
// that implements the sink interface and performs no I/O. It reports hook
// overhead only; it is deliberately NOT a persistence throughput number.
//
// The measured window of one round is hook + drain only; re-opening a fresh
// database between rounds happens with the timer stopped, so rounds stay
// independent and the tables do not grow without bound.
//
// Run:
//
//	go test ./internal/plugin -run '^$' -bench BenchmarkPlugin -benchmem -count=3

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mars160/cpa-distill/internal/storage"
)

const (
	benchPluginHookBody  = 2 << 10 // request.intercept_before body
	benchPluginRespBody  = 24 << 10
	benchPluginMarker    = 24
	benchPluginChunks    = 8
	benchPluginChunkBody = (benchPluginRespBody - benchPluginMarker) / benchPluginChunks // 3069
	benchPluginRequests  = 32
	benchPluginKeyPool   = 8
	benchPluginDrainWait = 60 * time.Second
)

var benchPluginConcurrency = []int{1, 2, 4, 8}

// benchPluginLoad is one fixed request shape. hookBody, respBody and chunkBody
// are the body sizes actually carried by the hooks.
type benchPluginLoad struct {
	name      string
	chunks    int
	events    int
	hookBody  int
	respBody  int
	chunkBody int
}

var benchPluginLoads = []benchPluginLoad{
	{name: "plain", chunks: 0, events: 4, hookBody: benchPluginHookBody, respBody: benchPluginRespBody},
	{name: "stream", chunks: benchPluginChunks, events: 4 + benchPluginChunks, hookBody: benchPluginHookBody, respBody: benchPluginMarker, chunkBody: benchPluginChunkBody},
}

func benchJSON(v any) []byte {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return raw
}

// benchRaw builds the deterministic body the host sends, byte for byte.
func benchRaw(tag string, n int) []byte {
	raw := make([]byte, n)
	for i := range raw {
		raw[i] = byte(0x20 + (i+len(tag))%95)
	}
	return raw
}

// benchBase64 is benchRaw encoded as base64, exactly like the real proxy does
// inside the hook JSON.
func benchBase64(tag string, n int) string {
	return base64.StdEncoding.EncodeToString(benchRaw(tag, n))
}

// benchRequestID is fixed width so every envelope of a round has the same size.
func benchRequestID(worker, iter int) string { return fmt.Sprintf("req-%02d%02d", worker, iter) }

// benchPluginKey returns a key from a fixed pool, so the key cardinality of a
// round does not depend on the concurrency level.
func benchPluginKey(index int) string {
	return fmt.Sprintf("sk-bench-%02d-0123456789abcdef", index%benchPluginKeyPool)
}

// benchEnvelope builds one real hook envelope. The "Chunk" marker selects the
// response.intercept_stream_chunk shape: the bytes travel in Body with
// ChunkIndex 0, matching the host contract and the corrected baseline envelope.
func benchEnvelope(requestID, scope, field, body string, withHeaders bool) []byte {
	fields := map[string]any{
		"RequestID": requestID,
		"Metadata":  map[string]any{"caller_scope": scope},
		field:       body,
	}
	if field == "Chunk" {
		delete(fields, field)
		fields["Body"] = body
		fields["ChunkIndex"] = 0
	}
	if withHeaders {
		fields["Headers"] = map[string][]string{"Authorization": {"Bearer " + scope}}
	}
	return benchJSON(fields)
}

// benchExpectedBodies is what the store must hold after one request of this
// shape: the client's original request body, and the non-stream response body or
// the concatenated stream chunk bodies (the marker is never stored).
func benchExpectedBodies(load benchPluginLoad) (request, response []byte) {
	request = benchRaw("hook", load.hookBody)
	if load.chunks > 0 {
		for c := 0; c < load.chunks; c++ {
			response = append(response, benchRaw("chunk", load.chunkBody)...)
		}
	} else {
		response = benchRaw("resp", load.respBody)
	}
	return request, response
}

// benchStoredBytes is the raw request+response bytes one request of this shape
// writes to SQLite.
func benchStoredBytes(load benchPluginLoad) int {
	request, response := benchExpectedBodies(load)
	return len(request) + len(response)
}

// benchSentBytes returns the bytes the host sends per request: the full
// envelopes, Metadata and Headers included.
func benchSentBytes(load benchPluginLoad) int {
	id := benchRequestID(0, 0)
	key := benchPluginKey(0)
	scope := keyID(key)
	hook := benchBase64("hook", load.hookBody)
	resp := benchBase64("resp", load.respBody)
	chunk := benchBase64("chunk", load.chunkBody)

	total := len(benchEnvelope(id, scope, "Body", hook, true))
	total += len(benchEnvelope(id, scope, "Body", hook, false))
	total += len(benchEnvelope(id, scope, "Body", resp, false))
	total += load.chunks * len(benchEnvelope(id, scope, "Chunk", chunk, false))
	total += len(benchCompleteEnvelope(id, scope))
	total += len(benchUsageEnvelope(key))
	return total
}

func benchCompleteEnvelope(requestID, scope string) []byte {
	return benchJSON(map[string]any{
		"RequestID": requestID,
		"Metadata":  map[string]any{"caller_scope": scope},
		"Outcome":   "succeeded",
	})
}

func benchUsageEnvelope(key string) []byte {
	return benchJSON(map[string]any{"APIKey": key})
}

// benchRequest is one fully prepared request: the exact hook envelopes and the
// client key. Nothing here is built inside the timed window.
type benchRequest struct {
	before   []byte
	after    []byte
	resp     []byte
	chunks   [][]byte
	complete []byte
	usage    []byte
}

// benchBuildRequests prepares benchPluginRequests requests; index i is the flat
// request index of a round.
func benchBuildRequests(load benchPluginLoad) []benchRequest {
	hook := benchBase64("hook", load.hookBody)
	resp := benchBase64("resp", load.respBody)
	chunk := benchBase64("chunk", load.chunkBody)

	reqs := make([]benchRequest, benchPluginRequests)
	for i := range reqs {
		worker, iter := i/32, i%32
		id := benchRequestID(worker, iter)
		key := benchPluginKey(i)
		scope := keyID(key)
		r := benchRequest{
			before:   benchEnvelope(id, scope, "Body", hook, true),
			after:    benchEnvelope(id, scope, "Body", hook, false),
			resp:     benchEnvelope(id, scope, "Body", resp, false),
			complete: benchCompleteEnvelope(id, scope),
			usage:    benchUsageEnvelope(key),
		}
		for c := 0; c < load.chunks; c++ {
			r.chunks = append(r.chunks, benchEnvelope(id, scope, "Chunk", chunk, false))
		}
		reqs[i] = r
	}
	return reqs
}

// send replays one prepared request through the public hook API.
func (r benchRequest) send(p *Plugin) {
	p.Handle("request.intercept_before", r.before)
	p.Handle("request.intercept_after", r.after)
	p.Handle("response.intercept_after", r.resp)
	for _, chunk := range r.chunks {
		p.Handle("response.intercept_stream_chunk", chunk)
	}
	p.Handle("request.complete", r.complete)
	p.Handle("usage.handle", r.usage)
}

func benchPluginConfigYAML(path string) string {
	return fmt.Sprintf(
		"driver: sqlite\ndsn: %s\nwrite_timeout: 30s\ndrain_timeout: 30s\nmax_request_bytes: %d\nmax_buffer_bytes: %d\nmax_pending_requests: %d\nqueue_size: %d\nrequest_ttl: 30m\n",
		path, 64<<20, 1<<30, 4096, 4096,
	)
}

func benchPluginConfigure(b *testing.B, p *Plugin, path string) {
	b.Helper()
	resp := p.Handle("plugin.reconfigure", benchJSON(map[string][]byte{"config_yaml": []byte(benchPluginConfigYAML(path))}))
	var out struct {
		OK bool `json:"ok"`
	}
	if err := json.Unmarshal(resp, &out); err != nil || !out.OK {
		b.Fatalf("plugin.reconfigure(%s) failed: %s", path, resp)
	}
}

// benchPluginDrained reports whether every queued capture and key batch has been
// written. p.bytes is charged when a batch is queued and released only after the
// write finished, so zero means "nothing in flight".
func benchPluginDrained(p *Plugin) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.bytes == 0 && len(p.pending) == 0
}

func benchPluginWaitDrained(b *testing.B, p *Plugin) time.Duration {
	b.Helper()
	start := time.Now()
	for {
		if benchPluginDrained(p) {
			return time.Since(start)
		}
		if time.Since(start) > benchPluginDrainWait {
			b.Fatal("timed out waiting for the background writer to drain")
		}
		time.Sleep(100 * time.Microsecond)
	}
}

// benchPluginVerify counts back what was actually stored: one capture per
// request, the exact hook count, and the exact request/response bytes.
func benchPluginVerify(b *testing.B, p *Plugin, load benchPluginLoad) {
	b.Helper()
	p.mu.Lock()
	sink := p.current.sink
	p.mu.Unlock()
	store, ok := sink.(*storage.Store)
	if !ok {
		b.Fatalf("benchmark requires the real SQLite sink, got %T", sink)
	}
	ctx := context.Background()
	page, err := store.SearchRecords(ctx, storage.RecordFilter{Page: 1, PageSize: storage.MaxPageSize})
	if err != nil {
		b.Fatalf("SearchRecords: %v", err)
	}
	if page.Total != int64(benchPluginRequests) {
		b.Fatalf("persisted %d captures, want %d", page.Total, benchPluginRequests)
	}
	wantRequest, wantResponse := benchExpectedBodies(load)
	var requestBytes, responseBytes int
	for _, item := range page.Items {
		detail, err := store.RecordDetail(ctx, item.SessionID, item.RequestID)
		if err != nil {
			b.Fatalf("RecordDetail(%s/%s): %v", item.SessionID, item.RequestID, err)
		}
		if detail.Record.EventCount != int64(load.events) {
			b.Fatalf("capture %s/%s has %d hooks, want %d", item.SessionID, item.RequestID, detail.Record.EventCount, load.events)
		}
		if !bytes.Equal(detail.Bodies.RequestBody, wantRequest) {
			b.Fatalf("capture %s/%s request body = %d bytes, want %d", item.SessionID, item.RequestID, len(detail.Bodies.RequestBody), len(wantRequest))
		}
		if !bytes.Equal(detail.Bodies.ResponseBody, wantResponse) {
			b.Fatalf("capture %s/%s response body = %d bytes, want %d", item.SessionID, item.RequestID, len(detail.Bodies.ResponseBody), len(wantResponse))
		}
		if len(detail.Bodies.WebSocketBody) != 0 {
			b.Fatalf("capture %s/%s stored %d websocket bytes, want 0", item.SessionID, item.RequestID, len(detail.Bodies.WebSocketBody))
		}
		requestBytes += len(detail.Bodies.RequestBody)
		responseBytes += len(detail.Bodies.ResponseBody)
	}
	if want := int64(benchPluginRequests * (len(wantRequest) + len(wantResponse))); int64(requestBytes+responseBytes) != want {
		b.Fatalf("stored %d body bytes, want %d", requestBytes+responseBytes, want)
	}
}

// benchPluginRound runs one fixed round and returns the hook and drain durations.
// The benchmark timer is left stopped so the caller can verify and re-open the
// database outside the measured window.
func benchPluginRound(b *testing.B, p *Plugin, workers int, load benchPluginLoad, reqs []benchRequest) (time.Duration, time.Duration) {
	b.Helper()
	droppedBefore := p.dropped.Load()
	failuresBefore := p.writeFailures.Load()

	var wg sync.WaitGroup
	start := make(chan struct{})
	for w := 0; w < workers; w++ {
		count := benchPluginRequests / workers
		if w < benchPluginRequests%workers {
			count++
		}
		if count == 0 {
			continue
		}
		base := w * (benchPluginRequests / workers)
		wg.Add(1)
		go func(base, count int) {
			defer wg.Done()
			<-start
			for i := 0; i < count; i++ {
				reqs[base+i].send(p)
			}
		}(base, count)
	}

	hookStart := time.Now()
	close(start)
	wg.Wait()
	hookElapsed := time.Since(hookStart)

	drain := benchPluginWaitDrained(b, p)
	b.StopTimer()

	if got := p.dropped.Load() - droppedBefore; got != 0 {
		b.Fatalf("plugin dropped %d requests: reported throughput would be inflated by skipped work", got)
	}
	if got := p.writeFailures.Load() - failuresBefore; got != 0 {
		b.Fatalf("plugin reported %d write failures", got)
	}
	if !benchPluginDrained(p) {
		b.Fatal("writer is not idle after drain")
	}
	benchPluginVerify(b, p, load)
	return hookElapsed, drain
}

// BenchmarkPluginEndToEnd measures the full hook + persistence pipeline for the
// plain and multi-chunk stream shapes at concurrency 1, 2, 4 and 8.
func BenchmarkPluginEndToEnd(b *testing.B) {
	for _, load := range benchPluginLoads {
		for _, workers := range benchPluginConcurrency {
			b.Run(fmt.Sprintf("%s/concurrency=%d", load.name, workers), func(b *testing.B) {
				dir := b.TempDir()
				p := New()
				defer func() { _ = p.Close() }()
				benchPluginConfigure(b, p, filepath.Join(dir, "round0.db"))

				reqs := benchBuildRequests(load)
				storedBytes := benchStoredBytes(load)
				sentBytes := benchSentBytes(load)
				b.SetBytes(int64(storedBytes * benchPluginRequests))
				b.ReportAllocs()
				var hookTotal, drainTotal time.Duration

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					h, d := benchPluginRound(b, p, workers, load, reqs)
					hookTotal += h
					drainTotal += d
					// Fresh database per round: keeps rounds independent and the
					// tables small, and stays outside the measured window.
					benchPluginConfigure(b, p, filepath.Join(dir, fmt.Sprintf("round%d.db", i+1)))
					b.StartTimer()
				}
				b.StopTimer()

				requests := float64(b.N * benchPluginRequests)
				elapsed := hookTotal.Seconds() + drainTotal.Seconds()
				b.ReportMetric(float64(storedBytes), "stored-B/req")
				b.ReportMetric(float64(sentBytes), "sent-B/req")
				b.ReportMetric(float64(hookTotal.Nanoseconds())/requests, "hook-ns/req")
				b.ReportMetric(float64(drainTotal.Nanoseconds())/requests, "drain-ns/req")
				if elapsed > 0 {
					b.ReportMetric(requests/elapsed, "persisted-req/s")
				}
			})
		}
	}
}

// benchNoopSink implements the sink interface and performs no I/O at all. Its
// counters are only read while holding Plugin.mu (via benchPluginWaitDrained),
// which the worker releases after each write, so the reads are ordered.
type benchNoopSink struct {
	mu       sync.Mutex
	captures int64
	keys     int64
}

func (s *benchNoopSink) AppendCapture(context.Context, storage.Capture, storage.CaptureBodies) error {
	s.mu.Lock()
	s.captures++
	s.mu.Unlock()
	return nil
}
func (s *benchNoopSink) UpsertClientKey(context.Context, string, string) error {
	s.mu.Lock()
	s.keys++
	s.mu.Unlock()
	return nil
}
func (s *benchNoopSink) Close() error { return nil }

func (s *benchNoopSink) snapshot() (captures, keys int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.captures, s.keys
}

// BenchmarkPluginHookOverhead measures the hook path alone, against a no-op sink.
// It reports hook-ns/req only: no persistence throughput is claimed, because
// nothing is persisted here. Compare it with BenchmarkPluginEndToEnd's hook-ns/req
// to separate hook cost from SQLite writer cost.
//
// Each round sends benchPluginKeyPool distinct usage keys repeatedly. Key upserts
// are deduplicated per backend, so exactly one upsert per distinct key is expected
// for the whole benchmark (the backend is never reconfigured here).
func BenchmarkPluginHookOverhead(b *testing.B) {
	for _, load := range benchPluginLoads {
		for _, workers := range benchPluginConcurrency {
			b.Run(fmt.Sprintf("%s/concurrency=%d", load.name, workers), func(b *testing.B) {
				sink := &benchNoopSink{}
				p := NewWithOpener(func(context.Context, storage.Config) (Sink, error) { return sink, nil })
				defer func() { _ = p.Close() }()
				benchPluginConfigure(b, p, filepath.Join(b.TempDir(), "noop.db"))

				reqs := benchBuildRequests(load)
				storedBytes := benchStoredBytes(load)
				sentBytes := benchSentBytes(load)
				b.SetBytes(int64(storedBytes * benchPluginRequests))
				b.ReportAllocs()
				var hookTotal, drainTotal time.Duration

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					capturesBefore, _ := sink.snapshot()
					droppedBefore := p.dropped.Load()

					var wg sync.WaitGroup
					start := make(chan struct{})
					for w := 0; w < workers; w++ {
						count := benchPluginRequests / workers
						if w < benchPluginRequests%workers {
							count++
						}
						if count == 0 {
							continue
						}
						base := w * (benchPluginRequests / workers)
						wg.Add(1)
						go func(base, count int) {
							defer wg.Done()
							<-start
							for i := 0; i < count; i++ {
								reqs[base+i].send(p)
							}
						}(base, count)
					}
					started := time.Now()
					close(start)
					wg.Wait()
					hookTotal += time.Since(started)

					drainTotal += benchPluginWaitDrained(b, p)
					b.StopTimer()
					if got := p.dropped.Load() - droppedBefore; got != 0 {
						b.Fatalf("plugin dropped %d requests on the no-op sink", got)
					}
					capturesAfter, _ := sink.snapshot()
					if got := capturesAfter - capturesBefore; got != benchPluginRequests {
						b.Fatalf("no-op sink saw %d captures, want %d", got, benchPluginRequests)
					}
					b.StartTimer()
				}
				b.StopTimer()

				_, keyUpserts := sink.snapshot()
				if keyUpserts != benchPluginKeyPool {
					b.Fatalf("no-op sink saw %d key upserts, want %d distinct keys (deduplicated)", keyUpserts, benchPluginKeyPool)
				}

				requests := float64(b.N * benchPluginRequests)
				b.ReportMetric(float64(storedBytes), "stored-B/req")
				b.ReportMetric(float64(sentBytes), "sent-B/req")
				b.ReportMetric(float64(hookTotal.Nanoseconds())/requests, "hook-ns/req")
				b.ReportMetric(float64(drainTotal.Nanoseconds())/requests, "noop-drain-ns/req")
			})
		}
	}
}
