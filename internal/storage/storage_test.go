package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// binaryPayload mimics a raw request/response body carrying binary data, including
// NUL bytes and invalid UTF-8, so byte-for-byte fidelity can be asserted.
func binaryPayload() []byte {
	p := make([]byte, 0, 512)
	for i := 0; i < 256; i++ {
		p = append(p, byte(i))
	}
	p = append(p, []byte(`{"body":"\u0000AAECAwQ=","image":"/9j/4AAQ"}`)...)
	p = append(p, 0x00, 0xff, 0xfe, 0x00, 0x80)
	return p
}

// testCaptureID derives a deterministic 32-hex capture ID from a label, so a test
// can address a record by name while the production path keeps drawing real random
// IDs. Distinct labels always give distinct IDs.
func testCaptureID(label string) string {
	sum := sha256.Sum256([]byte("test-capture-id\x00" + label))
	return hex.EncodeToString(sum[:captureIDBytesLen])
}

// sqlOpenForTest opens the raw database/sql handle of one sqlite file, so a test
// can build an incompatible pre-existing schema before Open is called.
func sqlOpenForTest(path string) (*sql.DB, error) {
	return sql.Open(sqliteDriverName, sqliteFileDSN(path))
}

// shardDBs returns the raw handle of every shard of a store, so tests can inspect
// the physical layout without going through the API.
func shardDBs(store *Store) []*sql.DB {
	dbs := make([]*sql.DB, 0, len(store.shards))
	for _, sh := range store.shards {
		dbs = append(dbs, sh.db)
	}
	return dbs
}

// forEachShard runs one assertion against every shard of a store.
func forEachShard(t *testing.T, store *Store, check func(index int, db *sql.DB)) {
	t.Helper()
	for i, db := range shardDBs(store) {
		check(i, db)
	}
}

// openTempSQLite opens a single-writer store in a fresh data directory and returns
// the store with that directory.
func openTempSQLite(t *testing.T) (*Store, string) {
	t.Helper()
	return openTempSQLiteShards(t, 1)
}

// openTempSQLiteShards opens a store with writerCount shards in a fresh data
// directory.
func openTempSQLiteShards(t *testing.T, writerCount int) (*Store, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "data")
	store, err := Open(context.Background(), Config{Driver: DriverSQLite, DSN: dir, WriterCount: writerCount})
	if err != nil {
		t.Fatalf("Open(sqlite %s, writer_count %d): %v", dir, writerCount, err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, dir
}

// reopenTempSQLite reopens a data directory with the same writer count.
func reopenTempSQLite(t *testing.T, dir string, writerCount int) *Store {
	t.Helper()
	store, err := Open(context.Background(), Config{Driver: DriverSQLite, DSN: dir, WriterCount: writerCount})
	if err != nil {
		t.Fatalf("reopen(sqlite %s, writer_count %d): %v", dir, writerCount, err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func countInShards(t *testing.T, store *Store, query string, args ...any) int {
	t.Helper()
	total := 0
	forEachShard(t, store, func(index int, db *sql.DB) {
		var n int
		if err := db.QueryRowContext(context.Background(), query, args...).Scan(&n); err != nil {
			t.Fatalf("shard %d: %s: %v", index, query, err)
		}
		total += n
	})
	return total
}

func countCaptures(t *testing.T, store *Store) int {
	t.Helper()
	return countInShards(t, store, "SELECT COUNT(*) FROM captures")
}

func countBodies(t *testing.T, store *Store) int {
	t.Helper()
	return countInShards(t, store, "SELECT COUNT(*) FROM capture_bodies")
}

func countClientKeys(t *testing.T, store *Store) int {
	t.Helper()
	return countInShards(t, store, "SELECT COUNT(*) FROM client_keys")
}

// storedClientKey reads the plaintext key straight from the client_keys table of
// every shard, bypassing the API, to prove it is persisted even though no JSON
// exposes it. The key must be present in all shards: the mapping is cross-shard, so
// a shard that missed it would serve records without a label.
func storedClientKey(t *testing.T, store *Store, keyID string) string {
	t.Helper()
	value := ""
	forEachShard(t, store, func(index int, db *sql.DB) {
		var key string
		if err := db.QueryRowContext(context.Background(),
			"SELECT client_key FROM client_keys WHERE key_id = ?", keyID).Scan(&key); err != nil {
			t.Fatalf("shard %d: read stored client_key of scope %s: %v", index, keyID, err)
		}
		if value == "" {
			value = key
			return
		}
		if key != value {
			t.Fatalf("shard %d stores client_key %q, other shards store %q", index, key, value)
		}
	})
	return value
}

// clientKeyInShard reports the stored plaintext of one shard without requiring the
// row to exist.
func clientKeyInShard(t *testing.T, store *Store, index int, keyID string) (string, bool) {
	t.Helper()
	var key string
	err := shardDBs(store)[index].QueryRowContext(context.Background(),
		"SELECT client_key FROM client_keys WHERE key_id = ?", keyID).Scan(&key)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false
	}
	if err != nil {
		t.Fatalf("shard %d: read client_key of scope %s: %v", index, keyID, err)
	}
	return key, true
}

func TestSQLiteCaptureRoundTripSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "data")
	request := binaryPayload()
	response := []byte(`{"choices":[{"delta":{"content":"hi"}}]}`)
	websocket := []byte{0x00, 0xff, 0xfe, 0x00, 0x80}
	startedAt := time.Date(2026, 2, 3, 4, 5, 6, 123456789, time.UTC)
	// The captures table stores epoch microseconds, so the nanosecond part is
	// truncated on write.
	startedAt = startedAt.Truncate(time.Microsecond)

	store, err := Open(ctx, Config{Driver: DriverSQLite, DSN: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got := store.Driver(); got != DriverSQLite {
		t.Fatalf("Driver() = %q, want %q", got, DriverSQLite)
	}
	if got := store.WriterCount(); got != 1 {
		t.Fatalf("WriterCount() = %d, want 1", got)
	}
	if got := store.ShardCount(); got != 1 {
		t.Fatalf("ShardCount() = %d, want 1", got)
	}
	capture := Capture{
		CaptureID:      testCaptureID("round-trip"),
		RequestID:      "host-request-round-trip",
		ClientKey:      "sk-round-trip-0123456789",
		ClientKeyID:    "scope-1",
		Model:          "gpt-5",
		RequestedModel: "gpt-5-mini",
		SourceFormat:   "openai",
		Stream:         true,
		StartedAt:      startedAt,
		CompletedAt:    startedAt.Add(time.Second),
		EventCount:     7,
	}
	bodies := CaptureBodies{RequestBody: request, ResponseBody: response, WebSocketBody: websocket}
	if err := store.AppendCapture(ctx, capture, bodies); err != nil {
		t.Fatalf("AppendCapture: %v", err)
	}
	// An empty host request ID is allowed: not every hook maps to a model execution.
	empty := Capture{
		CaptureID:   testCaptureID("no-request-id"),
		StartedAt:   startedAt,
		CompletedAt: startedAt.Add(time.Second),
	}
	if err := store.AppendCapture(ctx, empty, CaptureBodies{RequestBody: []byte{}, ResponseBody: nil}); err != nil {
		t.Fatalf("AppendCapture(no request ID): %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened := reopenTempSQLite(t, dir, 1)
	detail, err := reopened.RecordDetail(ctx, capture.CaptureID)
	if err != nil {
		t.Fatalf("RecordDetail: %v", err)
	}
	got := detail.Record
	if got.CaptureID != capture.CaptureID || got.RequestID != capture.RequestID || got.ClientKeyID != capture.ClientKeyID {
		t.Fatalf("metadata mismatch after reopen: %+v", got)
	}
	if got.Model != capture.Model || got.RequestedModel != capture.RequestedModel || got.SourceFormat != capture.SourceFormat || !got.Stream {
		t.Fatalf("summary mismatch after reopen: %+v", got)
	}
	if got.EventCount != capture.EventCount {
		t.Fatalf("EventCount = %d, want %d", got.EventCount, capture.EventCount)
	}
	if !got.StartedAt.Equal(startedAt) || got.StartedAt.Nanosecond() != startedAt.Nanosecond() {
		t.Fatalf("StartedAt = %s, want %s", got.StartedAt.Format(time.RFC3339Nano), startedAt.Format(time.RFC3339Nano))
	}
	if got.StartedAt.Location() != time.UTC {
		t.Fatalf("StartedAt location = %s, want UTC", got.StartedAt.Location())
	}
	if got.ClientKey != "" {
		t.Fatalf("RecordDetail returned the plaintext client key: %q", got.ClientKey)
	}
	if !bytes.Equal(detail.Bodies.RequestBody, request) {
		t.Fatalf("request body not byte-exact: got %d bytes, want %d", len(detail.Bodies.RequestBody), len(request))
	}
	if !bytes.Equal(detail.Bodies.ResponseBody, response) {
		t.Fatalf("response body not byte-exact: %q", detail.Bodies.ResponseBody)
	}
	if !bytes.Equal(detail.Bodies.WebSocketBody, websocket) {
		t.Fatalf("websocket body not byte-exact: %v", detail.Bodies.WebSocketBody)
	}

	// GetRecord returns the same summary without the bodies.
	summary, err := reopened.GetRecord(ctx, capture.CaptureID)
	if err != nil {
		t.Fatalf("GetRecord: %v", err)
	}
	if summary != detail.Record {
		t.Fatalf("GetRecord = %+v, want %+v", summary, detail.Record)
	}

	emptyDetail, err := reopened.RecordDetail(ctx, empty.CaptureID)
	if err != nil {
		t.Fatalf("RecordDetail(no request ID): %v", err)
	}
	if len(emptyDetail.Bodies.RequestBody) != 0 || len(emptyDetail.Bodies.ResponseBody) != 0 || emptyDetail.Record.EventCount != 0 {
		t.Fatalf("empty capture mismatch: %+v", emptyDetail)
	}

	if _, err := reopened.RecordDetail(ctx, testCaptureID("missing")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("RecordDetail(missing) error = %v, want ErrNotFound", err)
	}
	if _, err := reopened.GetRecord(ctx, testCaptureID("missing")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetRecord(missing) error = %v, want ErrNotFound", err)
	}
}

func TestSQLiteDuplicateCaptureDoesNotOverwrite(t *testing.T) {
	ctx := context.Background()
	store, _ := openTempSQLite(t)
	startedAt := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)

	first := Capture{CaptureID: testCaptureID("dup"), Model: "original", StartedAt: startedAt, CompletedAt: startedAt.Add(time.Second)}
	if err := store.AppendCapture(ctx, first, CaptureBodies{RequestBody: []byte("original")}); err != nil {
		t.Fatalf("AppendCapture(first): %v", err)
	}
	retry := first
	retry.Model = "retry-must-not-win"
	if err := store.AppendCapture(ctx, retry, CaptureBodies{RequestBody: []byte("retry-must-not-win")}); err == nil {
		t.Fatal("AppendCapture(duplicate capture ID) succeeded, want constraint error")
	}

	detail, err := store.RecordDetail(ctx, first.CaptureID)
	if err != nil {
		t.Fatalf("RecordDetail: %v", err)
	}
	if detail.Record.Model != "original" {
		t.Fatalf("summary was overwritten by the retry: %q", detail.Record.Model)
	}
	if string(detail.Bodies.RequestBody) != "original" {
		t.Fatalf("bodies were overwritten by the retry: %q", detail.Bodies.RequestBody)
	}
	if got := countCaptures(t, store); got != 1 {
		t.Fatalf("stored %d captures, want 1", got)
	}
	if got := countBodies(t, store); got != 1 {
		t.Fatalf("stored %d body rows, want 1", got)
	}
}

func TestSQLiteOpenIsIdempotentAndKeepsData(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "data")

	for i := 0; i < 3; i++ {
		store, err := Open(ctx, Config{Driver: DriverSQLite, DSN: dir})
		if err != nil {
			t.Fatalf("Open #%d: %v", i+1, err)
		}
		at := time.Date(2026, 1, 1, 0, 0, i, 0, time.UTC)
		if err := store.AppendCapture(ctx, Capture{
			CaptureID:   testCaptureID("idempotent-" + string(rune('a'+i))),
			StartedAt:   at,
			CompletedAt: at.Add(time.Second),
		}, CaptureBodies{RequestBody: []byte{byte(i), 0x00}}); err != nil {
			t.Fatalf("AppendCapture #%d: %v", i+1, err)
		}
		if err := store.Close(); err != nil {
			t.Fatalf("Close #%d: %v", i+1, err)
		}
	}

	store := reopenTempSQLite(t, dir, 1)
	if got := countCaptures(t, store); got != 3 {
		t.Fatalf("stored %d captures after 3 opens, want 3", got)
	}
	if got := countBodies(t, store); got != 3 {
		t.Fatalf("stored %d body rows after 3 opens, want 3", got)
	}

	forEachShard(t, store, func(index int, db *sql.DB) {
		var mode string
		if err := db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode); err != nil {
			t.Fatalf("shard %d journal_mode: %v", index, err)
		}
		if !strings.EqualFold(strings.TrimSpace(mode), "wal") {
			t.Fatalf("shard %d journal_mode = %q, want wal", index, mode)
		}
	})
}

// TestSQLiteSchemaHasExactlyTheDesignedIndexes pins the index set of every shard:
// the summary sort composite and the (client_key_id, started_at_us) probe. Bodies
// and keys must not carry any extra index, only their primary key.
func TestSQLiteSchemaHasExactlyTheDesignedIndexes(t *testing.T) {
	store, _ := openTempSQLiteShards(t, 3)
	forEachShard(t, store, func(index int, db *sql.DB) {
		rows, err := db.QueryContext(context.Background(),
			"SELECT tbl_name, name FROM sqlite_master WHERE type = 'index' AND name NOT LIKE 'sqlite_autoindex%' ORDER BY tbl_name, name")
		if err != nil {
			t.Fatalf("shard %d: list indexes: %v", index, err)
		}
		defer rows.Close()
		got := map[string]string{}
		for rows.Next() {
			var table, name string
			if err := rows.Scan(&table, &name); err != nil {
				t.Fatalf("shard %d: scan index: %v", index, err)
			}
			got[name] = table
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("shard %d: index rows: %v", index, err)
		}
		want := map[string]string{
			"captures_started_at_us_capture_id_idx":    "captures",
			"captures_client_key_id_started_at_us_idx": "captures",
		}
		if len(got) != len(want) {
			t.Fatalf("shard %d indexes = %v, want exactly %v", index, got, want)
		}
		for name, table := range want {
			if got[name] != table {
				t.Errorf("shard %d: index %q on %q missing; found %v", index, name, table, got)
			}
		}
	})
}

func TestSQLiteCloseSemantics(t *testing.T) {
	ctx := context.Background()
	store, _ := openTempSQLite(t)

	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	at := time.Now().UTC()
	if err := store.AppendCapture(ctx, Capture{CaptureID: testCaptureID("closed"), StartedAt: at, CompletedAt: at}, CaptureBodies{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("AppendCapture after Close error = %v, want ErrClosed", err)
	}
	if err := store.UpsertClientKey(ctx, "scope", "sk-key-0123456789"); !errors.Is(err, ErrClosed) {
		t.Fatalf("UpsertClientKey after Close error = %v, want ErrClosed", err)
	}
	if _, err := store.SearchRecords(ctx, RecordFilter{Page: 1, PageSize: 1}); !errors.Is(err, ErrClosed) {
		t.Fatalf("SearchRecords after Close error = %v, want ErrClosed", err)
	}
	if _, err := store.RecordOptions(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("RecordOptions after Close error = %v, want ErrClosed", err)
	}
	if _, err := store.RecordDetail(ctx, testCaptureID("closed")); !errors.Is(err, ErrClosed) {
		t.Fatalf("RecordDetail after Close error = %v, want ErrClosed", err)
	}
	if _, err := store.GetRecord(ctx, testCaptureID("closed")); !errors.Is(err, ErrClosed) {
		t.Fatalf("GetRecord after Close error = %v, want ErrClosed", err)
	}
	if got := store.Driver(); got != DriverSQLite {
		t.Fatalf("Driver() after Close = %q", got)
	}
}

func TestOpenRejectsBadConfig(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "data")
	cases := []Config{
		{},
		{Driver: "mysql", DSN: "x"},
		{Driver: DriverSQLite},
		{Driver: DriverPostgres},
		{Driver: DriverSQLite, DSN: dir, WriterCount: -1},
		{Driver: DriverSQLite, DSN: dir, WriterCount: MaxWriterCount + 1},
	}
	for _, cfg := range cases {
		if _, err := Open(ctx, cfg); err == nil {
			t.Errorf("Open(%+v) succeeded, want error", cfg)
		}
	}
	// A rejected writer_count must not have created anything on disk.
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a rejected configuration touched %s: %v", dir, err)
	}
}

// TestSQLiteOpenRejectsIncompatibleSchema keeps the loud-failure behaviour: a
// pre-existing table that does not match this version is reported as an error and
// never silently migrated or dropped.
func TestSQLiteOpenRejectsIncompatibleSchema(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "data")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("create data directory: %v", err)
	}
	if err := writeManifest(dir, 1); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	raw, err := sqlOpenForTest(filepath.Join(dir, shardFileName(0)))
	if err != nil {
		t.Fatalf("open raw sqlite: %v", err)
	}
	if _, err := raw.ExecContext(ctx, "CREATE TABLE captures (session_id TEXT NOT NULL, request_id TEXT NOT NULL, client_key_id TEXT NOT NULL DEFAULT '', started_at_us INTEGER NOT NULL, PRIMARY KEY (session_id, request_id))"); err != nil {
		t.Fatalf("create incompatible captures table: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw sqlite: %v", err)
	}

	if _, err := Open(ctx, Config{Driver: DriverSQLite, DSN: dir}); err == nil {
		t.Fatal("Open(incompatible schema) succeeded, want an error")
	} else if !strings.Contains(err.Error(), "incompatible") {
		t.Fatalf("Open(incompatible schema) error = %v, want an incompatibility report", err)
	}
}
