package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// TestPostgresIntegration exercises the real PostgreSQL driver. It only runs when
// CPA_DISTILL_TEST_POSTGRES_DSN is set explicitly; otherwise it is skipped so the
// default test run needs no database.
func TestPostgresIntegration(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("CPA_DISTILL_TEST_POSTGRES_DSN"))
	if dsn == "" {
		t.Skip("CPA_DISTILL_TEST_POSTGRES_DSN is not set: skipping PostgreSQL integration test")
	}

	ctx := context.Background()
	store, err := Open(ctx, Config{Driver: DriverPostgres, DSN: dsn})
	if err != nil {
		t.Fatalf("Open(postgres): %v", err)
	}
	defer store.Close()

	if got := store.Driver(); got != DriverPostgres {
		t.Fatalf("Driver() = %q, want %q", got, DriverPostgres)
	}

	sessionID := fmt.Sprintf("cpa-distill-test-%d", time.Now().UnixNano())
	keyID := sessionID + "-key-id"
	t.Cleanup(func() {
		cleanup := []struct {
			query string
			args  []any
		}{
			{"DELETE FROM capture_bodies WHERE session_id = $1", []any{sessionID}},
			{"DELETE FROM captures WHERE session_id = $1", []any{sessionID}},
			{"DELETE FROM client_keys WHERE key_id = $1", []any{keyID}},
		}
		for _, c := range cleanup {
			if _, err := store.db.ExecContext(context.Background(), c.query, c.args...); err != nil {
				t.Errorf("cleanup %s: %v", c.query, err)
			}
		}
	})

	requestBody := binaryPayload()
	responseBody := []byte(`{"id":"chatcmpl-pg","choices":[]}`)
	startedAt := time.Date(2026, 2, 3, 4, 5, 6, 123456000, time.UTC)
	const clientKey = "sk-postgres-integration-key"

	capture := Capture{
		SessionID:      sessionID,
		RequestID:      sessionID + "-req-1",
		ClientKey:      clientKey,
		ClientKeyID:    keyID,
		Model:          "gpt-5",
		RequestedModel: "gpt-5-mini",
		SourceFormat:   "openai",
		Stream:         true,
		StartedAt:      startedAt,
		CompletedAt:    startedAt.Add(2 * time.Second),
		EventCount:     12,
	}
	if err := store.AppendCapture(ctx, capture, CaptureBodies{
		RequestBody:   requestBody,
		ResponseBody:  responseBody,
		WebSocketBody: []byte{0x00, 0xff},
	}); err != nil {
		t.Fatalf("AppendCapture: %v", err)
	}
	if err := store.AppendCapture(ctx, capture, CaptureBodies{}); err == nil {
		t.Fatal("AppendCapture with a duplicate (session_id, request_id) succeeded, want unique violation")
	}

	// The list, the filter options and the detail view all read the same record.
	page, err := store.SearchRecords(ctx, RecordFilter{KeyID: keyID, Model: "gpt-5-mini", From: startedAt.Add(-time.Second), To: startedAt.Add(time.Second), Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("SearchRecords: %v", err)
	}
	if page.Total != 1 || len(page.Items) != 1 {
		t.Fatalf("SearchRecords matched %d records (%d items), want 1", page.Total, len(page.Items))
	}
	got := page.Items[0]
	if got.ClientKeyID != keyID || got.EventCount != 12 || !got.Stream || got.ClientKey != clientKey {
		t.Fatalf("record = %+v", got)
	}

	options, err := store.RecordOptions(ctx)
	if err != nil {
		t.Fatalf("RecordOptions: %v", err)
	}
	foundKey, foundModel := false, false
	for _, k := range options.Keys {
		if k.ID == keyID {
			foundKey = true
			if k.Label == clientKey || strings.Contains(k.Label, clientKey) {
				t.Fatalf("key label %q leaks the client key", k.Label)
			}
		}
	}
	for _, m := range options.Models {
		if m == "gpt-5-mini" {
			foundModel = true
		}
	}
	if !foundKey || !foundModel {
		t.Fatalf("RecordOptions missed the stored key (%v) or model (%v)", foundKey, foundModel)
	}

	detail, err := store.RecordDetail(ctx, sessionID, capture.RequestID)
	if err != nil {
		t.Fatalf("RecordDetail: %v", err)
	}
	if !bytes.Equal(detail.Bodies.RequestBody, requestBody) {
		t.Fatalf("detail request body not byte-exact: got %d bytes, want %d", len(detail.Bodies.RequestBody), len(requestBody))
	}
	if !bytes.Equal(detail.Bodies.ResponseBody, responseBody) {
		t.Fatalf("detail response body = %q", detail.Bodies.ResponseBody)
	}
	if detail.Record.ClientKey != "" {
		t.Fatalf("RecordDetail returned the plaintext client key: %q", detail.Record.ClientKey)
	}
	if _, err := store.RecordDetail(ctx, sessionID, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("RecordDetail(missing) error = %v, want ErrNotFound", err)
	}

	// A changed plaintext only rewrites client_keys: the capture row and its bodies
	// stay exactly as they were.
	before := detail.Record
	const updatedKey = "sk-postgres-updated-key"
	if err := store.UpsertClientKey(ctx, keyID, clientKey); err != nil {
		t.Fatalf("UpsertClientKey(same key): %v", err)
	}
	if err := store.UpsertClientKey(ctx, keyID, updatedKey); err != nil {
		t.Fatalf("UpsertClientKey(new key): %v", err)
	}
	after, err := store.RecordDetail(ctx, sessionID, capture.RequestID)
	if err != nil {
		t.Fatalf("RecordDetail after upsert: %v", err)
	}
	if after.Record != before {
		t.Fatalf("upserting a key rewrote the capture:\n before %+v\n after  %+v", before, after.Record)
	}
	if !bytes.Equal(after.Bodies.RequestBody, requestBody) {
		t.Fatalf("upserting a key rewrote the bodies")
	}
	page, err = store.SearchRecords(ctx, RecordFilter{KeyID: keyID, Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("SearchRecords after upsert: %v", err)
	}
	if len(page.Items) != 1 || page.Items[0].ClientKey != updatedKey {
		t.Fatalf("list did not join the updated key: %+v", page.Items)
	}

	// The scope must be a real one and the key must be non-empty.
	for _, bad := range []struct {
		name       string
		keyID, key string
	}{
		{"empty scope", "", clientKey},
		{"unknown scope", UnknownKeyID, clientKey},
		{"empty key", keyID, ""},
	} {
		if err := store.UpsertClientKey(ctx, bad.keyID, bad.key); err == nil {
			t.Errorf("UpsertClientKey(%s) succeeded, want validation error", bad.name)
		}
	}
}
