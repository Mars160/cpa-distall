package storage

// Performance baseline for the SQLite engine of the distill plugin.
//
// The benchmarks always use a real temporary file (never ":memory:") so the WAL,
// the single-connection pool and the on-disk indexes behave exactly as in
// production. benchOpenStore asserts that journal_mode really is WAL, so a
// baseline can never be silently recorded against a rollback-journal database.
//
// The logical load is fixed and identical for both request shapes. It is the same
// body byte volume per request as the baseline, only redistributed over the three
// CaptureBodies fields (the summary/body/client-key split does not change how many
// bytes are written):
//
//	request body               4,119 B  (2 KiB + 2 KiB request + 23 B completion marker)
//	response body             24,576 B  (plain) or 24 B marker (stream)
//	websocket body                 0 B  (plain) or 24,552 B = 8 x 3,069 B (stream)
//	total                     28,695 B  per request, plain and stream alike
//	concurrency                1, 2, 4, 8 concurrent callers
//
// The baseline document rounds the per-request total to 28,694 B (it counted the
// completion marker as 22 B); the baseline code used len() of the 23 B literal, so
// 28,695 B is what its SetBytes and MB/s numbers were computed from and what the
// reworked benchmark keeps to stay comparable.
//
// Store serialises writers on one connection, so the concurrency levels describe
// how the engine behaves under concurrent callers, not real write parallelism.
//
// The sub-benchmark names (plain4/stream12) are kept identical to the
// baseline so the numbers stay comparable; they no longer count hook events, which
// the storage layer no longer stores one row per. The event-only
// BenchmarkSQLiteAppendBatch was removed with the events table: with the new model
// every write is a summary plus one body row, which is exactly what
// BenchmarkSQLiteAppendCapture measures.
//
// Every benchmark is written against the exported Store API (AppendCapture,
// RecordDetail, SearchRecords), so the same logical load can be replayed unchanged.
//
// Run:
//
//	go test ./internal/storage -run '^$' -bench BenchmarkSQLite -benchmem -count=3

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	benchHookBodyBytes = 2 << 10
	benchRespBodyBytes = 24 << 10
	benchMarkerBytes   = 24
	benchChunkCount    = 8
	benchChunkBytes    = (benchRespBodyBytes - benchMarkerBytes) / benchChunkCount // 3069
	benchWSBodyBytes   = benchChunkBytes * benchChunkCount                         // 24552
	benchCompleteJSON  = `{"Outcome":"succeeded"}`                                 // 23 B
	// benchReqBodyBytes keeps the two request bodies and the completion marker of the
	// baseline so the per-request total stays exactly at the baseline value.
	benchReqBodyBytes = benchHookBodyBytes*2 + len(benchCompleteJSON) // 4119
	benchBodyBytes    = benchReqBodyBytes + benchRespBodyBytes        // 28695
)

// benchBase is a fixed timestamp so StartedAt never depends on wall-clock time and
// two runs of the benchmark store identical rows.
var benchBase = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

var benchLevels = []int{1, 2, 4, 8}

// benchLoad is one fixed request shape: how the baseline body bytes are split
// between the response and the websocket field (identical totals for both).
type benchLoad struct {
	name           string
	stream         bool
	responseBytes  int
	websocketBytes int
	payload        int
}

var benchLoads = []benchLoad{
	{name: "plain4", stream: false, responseBytes: benchRespBodyBytes, websocketBytes: 0, payload: benchBodyBytes},
	{name: "stream12", stream: true, responseBytes: benchMarkerBytes, websocketBytes: benchWSBodyBytes, payload: benchBodyBytes},
}

// benchPayload builds a deterministic payload of exactly n bytes. It is built
// once per sub-benchmark and shared by every operation, so no allocation cost
// leaks into the measured path.
func benchPayload(tag string, n int) []byte {
	p := make([]byte, n)
	for i := range p {
		p[i] = byte(0x20 + (i+len(tag))%95)
	}
	return p
}

// benchBodies builds the fixed body set of one request shape. The bytes are shared
// by every operation of the sub-benchmark.
func benchBodies(load benchLoad) CaptureBodies {
	return CaptureBodies{
		RequestBody:   benchPayload("request", benchReqBodyBytes),
		ResponseBody:  benchPayload("response", load.responseBytes),
		WebSocketBody: benchPayload("websocket", load.websocketBytes),
	}
}

// benchOpenStore opens a real SQLite file in a temporary directory and verifies
// that it runs in WAL mode.
func benchOpenStore(b *testing.B, name string) *Store {
	b.Helper()
	path := filepath.Join(b.TempDir(), name+".db")
	store, err := Open(context.Background(), Config{Driver: DriverSQLite, DSN: path})
	if err != nil {
		b.Fatalf("Open(sqlite %s): %v", path, err)
	}
	b.Cleanup(func() { _ = store.Close() })

	var mode string
	if err := store.db.QueryRowContext(context.Background(), "PRAGMA journal_mode").Scan(&mode); err != nil {
		b.Fatalf("PRAGMA journal_mode: %v", err)
	}
	if !strings.EqualFold(strings.TrimSpace(mode), "wal") {
		b.Fatalf("journal_mode = %q, want wal (benchmark must run on a WAL file)", mode)
	}
	return store
}

// benchRunConcurrent runs exactly b.N operations spread over workers goroutines
// that start together. Errors are collected and reported on the benchmark
// goroutine, because testing.B.Fatal must not be called from a worker.
func benchRunConcurrent(b *testing.B, workers int, op func(worker, iter int) error) {
	if workers < 1 {
		workers = 1
	}
	var (
		wg    sync.WaitGroup
		start = make(chan struct{})
		errs  = make([]error, workers)
	)
	for w := 0; w < workers; w++ {
		count := b.N / workers
		if w < b.N%workers {
			count++
		}
		if count == 0 {
			continue
		}
		wg.Add(1)
		go func(w, count int) {
			defer wg.Done()
			<-start
			for i := 0; i < count; i++ {
				if err := op(w, i); err != nil {
					errs[w] = err
					return
				}
			}
		}(w, count)
	}
	b.ResetTimer()
	close(start)
	wg.Wait()
	for w, err := range errs {
		if err != nil {
			b.Fatalf("worker %d: %v", w, err)
		}
	}
}

// TestBenchBodyTotalsMatchBaseline guards the comparability of the storage numbers:
// plain and stream must still carry exactly the baseline body byte volume per
// request, only redistributed over the CaptureBodies fields. The expected total is
// recomputed from the baseline formula (two request bodies + response body + the
// completion marker literal) rather than hard-coded, so it follows the baseline if
// the literal ever changes.
func TestBenchBodyTotalsMatchBaseline(t *testing.T) {
	const baselinePerRequest = benchHookBodyBytes*2 + benchRespBodyBytes + len(benchCompleteJSON)
	for _, load := range benchLoads {
		b := benchBodies(load)
		got := len(b.RequestBody) + len(b.ResponseBody) + len(b.WebSocketBody)
		if got != baselinePerRequest {
			t.Fatalf("%s stores %d body bytes, want %d", load.name, got, baselinePerRequest)
		}
		if load.payload != baselinePerRequest {
			t.Fatalf("%s declares %d bytes/op, want %d", load.name, load.payload, baselinePerRequest)
		}
	}
}

// BenchmarkSQLiteAppendCapture measures the production write path: one summary row
// plus the body row of the request in a single transaction, including the client
// key upsert. As in the baseline, each operation uses a distinct caller scope.
// Unlike the old inline-key layout, this inserts a new client_keys row per call;
// its growing-table cost is intentionally included (a conservative comparison).
// The plugin end-to-end benchmark separately uses a fixed pool of eight keys.
func BenchmarkSQLiteAppendCapture(b *testing.B) {
	for _, load := range benchLoads {
		for _, workers := range benchLevels {
			b.Run(fmt.Sprintf("%s/concurrency=%d", load.name, workers), func(b *testing.B) {
				store := benchOpenStore(b, "append-capture")
				bodies := benchBodies(load)
				b.SetBytes(int64(load.payload))
				b.ReportAllocs()
				benchRunConcurrent(b, workers, func(worker, iter int) error {
					capture := Capture{
						SessionID:      fmt.Sprintf("sess-%d", worker),
						RequestID:      fmt.Sprintf("req-%d", iter),
						ClientKey:      "sk-bench-client-key-0001",
						ClientKeyID:    fmt.Sprintf("scope-%d-%d", worker, iter),
						Model:          "gpt-bench",
						RequestedModel: "gpt-bench-alias",
						SourceFormat:   "openai",
						Stream:         load.stream,
						StartedAt:      benchBase,
						CompletedAt:    benchBase.Add(time.Second),
						EventCount:     12,
					}
					return store.AppendCapture(context.Background(), capture, bodies)
				})
			})
		}
	}
}

// BenchmarkSQLiteAppendCaptureDeferred measures the path where the client key is
// still unknown when the summary is written: the scope is stored but no key row is
// upserted.
func BenchmarkSQLiteAppendCaptureDeferred(b *testing.B) {
	load := benchLoads[1] // stream12
	for _, workers := range benchLevels {
		b.Run(fmt.Sprintf("stream12/concurrency=%d", workers), func(b *testing.B) {
			store := benchOpenStore(b, "append-capture-deferred")
			bodies := benchBodies(load)
			b.SetBytes(int64(load.payload))
			b.ReportAllocs()
			benchRunConcurrent(b, workers, func(worker, iter int) error {
				capture := Capture{
					SessionID:      fmt.Sprintf("sess-%d", worker),
					RequestID:      fmt.Sprintf("req-%d", iter),
					ClientKeyID:    fmt.Sprintf("scope-%d-%d", worker, iter),
					Model:          "gpt-bench",
					RequestedModel: "gpt-bench-alias",
					SourceFormat:   "openai",
					Stream:         load.stream,
					StartedAt:      benchBase,
					CompletedAt:    benchBase.Add(time.Second),
					EventCount:     12,
				}
				return store.AppendCapture(context.Background(), capture, bodies)
			})
		})
	}
}

// benchSeedRecords stores n stream12 captures (summary, bodies and key) so the read
// benchmarks query a realistically populated table.
func benchSeedRecords(b *testing.B, store *Store, n int) {
	b.Helper()
	load := benchLoads[1]
	bodies := benchBodies(load)
	ctx := context.Background()
	for i := 0; i < n; i++ {
		capture := Capture{
			SessionID:      fmt.Sprintf("seed-sess-%d", i/10),
			RequestID:      fmt.Sprintf("seed-req-%d", i),
			ClientKey:      "sk-bench-client-key-0001",
			ClientKeyID:    fmt.Sprintf("seed-scope-%d", i%8),
			Model:          "gpt-bench",
			RequestedModel: "gpt-bench-alias",
			SourceFormat:   "openai",
			Stream:         true,
			StartedAt:      benchBase.Add(time.Duration(i) * time.Millisecond),
			CompletedAt:    benchBase.Add(time.Duration(i)*time.Millisecond + time.Second),
			EventCount:     12,
		}
		if err := store.AppendCapture(ctx, capture, bodies); err != nil {
			b.Fatalf("seed capture %d: %v", i, err)
		}
	}
}

// BenchmarkSQLiteRecordDetail measures reading one summary plus its body row, the
// query behind the record detail view.
func BenchmarkSQLiteRecordDetail(b *testing.B) {
	store := benchOpenStore(b, "record-detail")
	const seeded = 200
	benchSeedRecords(b, store, seeded)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		idx := i % seeded
		if _, err := store.RecordDetail(context.Background(), fmt.Sprintf("seed-sess-%d", idx/10), fmt.Sprintf("seed-req-%d", idx)); err != nil {
			b.Fatalf("RecordDetail: %v", err)
		}
	}
}

// BenchmarkSQLiteSearchRecords measures one page of the record list, including
// the COUNT(*) that SearchRecords always performs.
func BenchmarkSQLiteSearchRecords(b *testing.B) {
	store := benchOpenStore(b, "search-records")
	benchSeedRecords(b, store, 200)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := store.SearchRecords(context.Background(), RecordFilter{Page: 1, PageSize: 20}); err != nil {
			b.Fatalf("SearchRecords: %v", err)
		}
	}
}
