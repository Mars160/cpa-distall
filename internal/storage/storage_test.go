package storage

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
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

// sqlOpenForTest opens the raw database/sql handle of a sqlite file, so a test can
// build an incompatible pre-existing schema before Open is called.
func sqlOpenForTest(path string) (*sql.DB, error) {
	return sql.Open(sqliteDriverName, sqliteDSN(path))
}

func openTempSQLite(t *testing.T) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "captures.db")
	store, err := Open(context.Background(), Config{Driver: DriverSQLite, DSN: path})
	if err != nil {
		t.Fatalf("Open(sqlite %s): %v", path, err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, path
}

func countCaptures(t *testing.T, store *Store) int {
	t.Helper()
	var total int
	if err := store.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM captures").Scan(&total); err != nil {
		t.Fatalf("count captures: %v", err)
	}
	return total
}

func countBodies(t *testing.T, store *Store) int {
	t.Helper()
	var total int
	if err := store.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM capture_bodies").Scan(&total); err != nil {
		t.Fatalf("count capture_bodies: %v", err)
	}
	return total
}

func countClientKeys(t *testing.T, store *Store) int {
	t.Helper()
	var total int
	if err := store.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM client_keys").Scan(&total); err != nil {
		t.Fatalf("count client_keys: %v", err)
	}
	return total
}

// storedClientKey reads the plaintext key straight from the client_keys table,
// bypassing the API, to prove it is persisted even though no JSON exposes it.
func storedClientKey(t *testing.T, store *Store, keyID string) string {
	t.Helper()
	var key string
	if err := store.db.QueryRowContext(context.Background(),
		"SELECT client_key FROM client_keys WHERE key_id = ?", keyID).Scan(&key); err != nil {
		t.Fatalf("read stored client_key of scope %s: %v", keyID, err)
	}
	return key
}

func TestSQLiteCaptureRoundTripSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "captures.db")
	request := binaryPayload()
	response := []byte(`{"choices":[{"delta":{"content":"hi"}}]}`)
	websocket := []byte{0x00, 0xff, 0xfe, 0x00, 0x80}
	startedAt := time.Date(2026, 2, 3, 4, 5, 6, 123456789, time.UTC)
	// The captures table stores epoch microseconds, so the nanosecond part is
	// truncated on write.
	startedAt = startedAt.Truncate(time.Microsecond)

	store, err := Open(ctx, Config{Driver: DriverSQLite, DSN: path})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got := store.Driver(); got != DriverSQLite {
		t.Fatalf("Driver() = %q, want %q", got, DriverSQLite)
	}
	capture := Capture{
		SessionID:      "sess-1",
		RequestID:      "req-1",
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
	// An empty request_id is allowed: not every hook maps to a model execution.
	if err := store.AppendCapture(ctx, Capture{
		SessionID:   "sess-1",
		StartedAt:   startedAt,
		CompletedAt: startedAt.Add(time.Second),
	}, CaptureBodies{RequestBody: []byte{}, ResponseBody: nil}); err != nil {
		t.Fatalf("AppendCapture(empty request_id): %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(ctx, Config{Driver: DriverSQLite, DSN: path})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	detail, err := reopened.RecordDetail(ctx, "sess-1", "req-1")
	if err != nil {
		t.Fatalf("RecordDetail: %v", err)
	}
	got := detail.Record
	if got.SessionID != capture.SessionID || got.RequestID != capture.RequestID || got.ClientKeyID != capture.ClientKeyID {
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

	empty, err := reopened.RecordDetail(ctx, "sess-1", "")
	if err != nil {
		t.Fatalf("RecordDetail(empty request_id): %v", err)
	}
	if len(empty.Bodies.RequestBody) != 0 || len(empty.Bodies.ResponseBody) != 0 || empty.Record.EventCount != 0 {
		t.Fatalf("empty capture mismatch: %+v", empty)
	}

	if _, err := reopened.RecordDetail(ctx, "sess-1", "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("RecordDetail(missing) error = %v, want ErrNotFound", err)
	}
}

func TestSQLiteDuplicateCaptureDoesNotOverwrite(t *testing.T) {
	ctx := context.Background()
	store, _ := openTempSQLite(t)
	startedAt := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)

	first := Capture{SessionID: "sess", RequestID: "req", Model: "original", StartedAt: startedAt, CompletedAt: startedAt.Add(time.Second)}
	if err := store.AppendCapture(ctx, first, CaptureBodies{RequestBody: []byte("original")}); err != nil {
		t.Fatalf("AppendCapture(first): %v", err)
	}
	retry := first
	retry.Model = "retry-must-not-win"
	if err := store.AppendCapture(ctx, retry, CaptureBodies{RequestBody: []byte("retry-must-not-win")}); err == nil {
		t.Fatal("AppendCapture(duplicate pair) succeeded, want constraint error")
	}

	detail, err := store.RecordDetail(ctx, "sess", "req")
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
	path := filepath.Join(t.TempDir(), "captures.db")

	for i := 0; i < 3; i++ {
		store, err := Open(ctx, Config{Driver: DriverSQLite, DSN: path})
		if err != nil {
			t.Fatalf("Open #%d: %v", i+1, err)
		}
		at := time.Date(2026, 1, 1, 0, 0, i, 0, time.UTC)
		if err := store.AppendCapture(ctx, Capture{
			SessionID:   "sess",
			RequestID:   "req-" + string(rune('a'+i)),
			StartedAt:   at,
			CompletedAt: at.Add(time.Second),
		}, CaptureBodies{RequestBody: []byte{byte(i), 0x00}}); err != nil {
			t.Fatalf("AppendCapture #%d: %v", i+1, err)
		}
		if err := store.Close(); err != nil {
			t.Fatalf("Close #%d: %v", i+1, err)
		}
	}

	store, err := Open(ctx, Config{Driver: DriverSQLite, DSN: path})
	if err != nil {
		t.Fatalf("final Open: %v", err)
	}
	defer store.Close()

	if got := countCaptures(t, store); got != 3 {
		t.Fatalf("stored %d captures after 3 opens, want 3", got)
	}
	if got := countBodies(t, store); got != 3 {
		t.Fatalf("stored %d body rows after 3 opens, want 3", got)
	}

	var mode string
	if err := store.db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode); err != nil {
		t.Fatalf("journal_mode: %v", err)
	}
	if !strings.EqualFold(strings.TrimSpace(mode), "wal") {
		t.Fatalf("journal_mode = %q, want wal", mode)
	}
}

// TestSQLiteSchemaHasExactlyTheDesignedIndexes pins the index set: the summary sort
// composite and the (client_key_id, started_at_us) probe. Bodies and keys must not
// carry any extra index, only their primary key.
func TestSQLiteSchemaHasExactlyTheDesignedIndexes(t *testing.T) {
	store, _ := openTempSQLite(t)
	rows, err := store.db.QueryContext(context.Background(),
		"SELECT tbl_name, name FROM sqlite_master WHERE type = 'index' AND name NOT LIKE 'sqlite_autoindex%' ORDER BY tbl_name, name")
	if err != nil {
		t.Fatalf("list indexes: %v", err)
	}
	defer rows.Close()
	got := map[string]string{}
	for rows.Next() {
		var table, name string
		if err := rows.Scan(&table, &name); err != nil {
			t.Fatalf("scan index: %v", err)
		}
		got[name] = table
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("index rows: %v", err)
	}
	want := map[string]string{
		"captures_started_at_us_session_id_request_id_idx": "captures",
		"captures_client_key_id_started_at_us_idx":         "captures",
	}
	if len(got) != len(want) {
		t.Fatalf("indexes = %v, want exactly %v", got, want)
	}
	for name, table := range want {
		if got[name] != table {
			t.Errorf("index %q on %q missing; found %v", name, table, got)
		}
	}
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
	if err := store.AppendCapture(ctx, Capture{SessionID: "s", StartedAt: at, CompletedAt: at}, CaptureBodies{}); !errors.Is(err, ErrClosed) {
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
	if _, err := store.RecordDetail(ctx, "s", "r"); !errors.Is(err, ErrClosed) {
		t.Fatalf("RecordDetail after Close error = %v, want ErrClosed", err)
	}
	if got := store.Driver(); got != DriverSQLite {
		t.Fatalf("Driver() after Close = %q", got)
	}
}

func TestOpenRejectsBadConfig(t *testing.T) {
	ctx := context.Background()
	cases := []Config{
		{},
		{Driver: "mysql", DSN: "x"},
		{Driver: DriverSQLite},
		{Driver: DriverPostgres},
	}
	for _, cfg := range cases {
		if _, err := Open(ctx, cfg); err == nil {
			t.Errorf("Open(%+v) succeeded, want error", cfg)
		}
	}
}

// TestSQLiteOpenRejectsIncompatibleSchema keeps the loud-failure behaviour: a
// pre-existing table that does not match this version is reported as an error and
// never silently migrated or dropped.
func TestSQLiteOpenRejectsIncompatibleSchema(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "incompatible.db")

	raw, err := sqlOpenForTest(path)
	if err != nil {
		t.Fatalf("open raw sqlite: %v", err)
	}
	if _, err := raw.ExecContext(ctx, "CREATE TABLE captures (session_id TEXT NOT NULL, request_id TEXT NOT NULL, client_key_id TEXT NOT NULL DEFAULT '', started_at_us INTEGER NOT NULL, PRIMARY KEY (session_id, request_id))"); err != nil {
		t.Fatalf("create incompatible captures table: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw sqlite: %v", err)
	}

	if _, err := Open(ctx, Config{Driver: DriverSQLite, DSN: path}); err == nil {
		t.Fatal("Open(incompatible schema) succeeded, want an error")
	} else if !strings.Contains(err.Error(), "incompatible") {
		t.Fatalf("Open(incompatible schema) error = %v, want an incompatibility report", err)
	}
}
