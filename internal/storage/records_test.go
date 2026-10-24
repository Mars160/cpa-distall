package storage

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// captureAt builds a summary whose timestamps are microsecond-aligned, the
// precision the captures table stores.
func captureAt(session, request, keyID, key, model, requested string, startedAt time.Time) Capture {
	return Capture{
		SessionID:      session,
		RequestID:      request,
		ClientKey:      key,
		ClientKeyID:    keyID,
		Model:          model,
		RequestedModel: requested,
		SourceFormat:   "openai",
		Stream:         true,
		StartedAt:      startedAt,
		CompletedAt:    startedAt.Add(1500 * time.Millisecond),
	}
}

func TestSQLiteAppendCaptureStoresSummaryWithBodies(t *testing.T) {
	ctx := context.Background()
	store, path := openTempSQLite(t)

	const (
		session = "sess-capture"
		request = "req-capture"
		keyID   = "3f1c9b0e5d2a4c6f8b1e3d5a7c9f0b2d4e6a8c0f1b3d5e7a9c1b3d5f7a9c1b3d"
		key     = "sk-live-abcdefghijklmnopqrstuvwxyz"
	)
	startedAt := time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)
	requestBody := binaryPayload()
	responseBody := []byte(`{"id":"chatcmpl-1","choices":[]}`)
	websocketBody := []byte{0x00, 0xff, 0xfe, 0x80}

	capture := captureAt(session, request, keyID, key, "gpt-5", "gpt-5-mini", startedAt)
	capture.EventCount = 12
	if err := store.AppendCapture(ctx, capture, CaptureBodies{
		RequestBody:   requestBody,
		ResponseBody:  responseBody,
		WebSocketBody: websocketBody,
	}); err != nil {
		t.Fatalf("AppendCapture: %v", err)
	}
	if got := countCaptures(t, store); got != 1 {
		t.Fatalf("stored %d captures, want 1", got)
	}
	if got := countBodies(t, store); got != 1 {
		t.Fatalf("stored %d body rows, want 1", got)
	}
	if got := storedClientKey(t, store, keyID); got != key {
		t.Fatalf("stored client key = %q, want the original key", got)
	}

	detail, err := store.RecordDetail(ctx, session, request)
	if err != nil {
		t.Fatalf("RecordDetail: %v", err)
	}
	if detail.Record.ClientKey != "" {
		t.Fatalf("RecordDetail returned the plaintext client key: %q", detail.Record.ClientKey)
	}
	want := capture
	want.ClientKey = ""
	if detail.Record != want {
		t.Fatalf("detail record = %+v, want %+v", detail.Record, want)
	}
	if !bytes.Equal(detail.Bodies.RequestBody, requestBody) {
		t.Fatalf("request body not byte-exact: got %d bytes, want %d", len(detail.Bodies.RequestBody), len(requestBody))
	}
	if !bytes.Equal(detail.Bodies.ResponseBody, responseBody) {
		t.Fatalf("response body = %q", detail.Bodies.ResponseBody)
	}
	if !bytes.Equal(detail.Bodies.WebSocketBody, websocketBody) {
		t.Fatalf("websocket body = %v", detail.Bodies.WebSocketBody)
	}

	// The summary and the bodies must survive a reopen together.
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened, err := Open(ctx, Config{Driver: DriverSQLite, DSN: path})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	again, err := reopened.RecordDetail(ctx, session, request)
	if err != nil {
		t.Fatalf("RecordDetail after reopen: %v", err)
	}
	if again.Record.Model != "gpt-5" || again.Record.EventCount != 12 {
		t.Fatalf("record after reopen = %+v", again.Record)
	}
	if !bytes.Equal(again.Bodies.RequestBody, requestBody) || !bytes.Equal(again.Bodies.WebSocketBody, websocketBody) {
		t.Fatalf("bodies after reopen are not byte-exact")
	}

	// A capture without a scope never writes a key row.
	if err := reopened.AppendCapture(ctx, Capture{
		SessionID:   session,
		RequestID:   "req-no-scope",
		ClientKey:   "sk-ignored-because-there-is-no-scope",
		StartedAt:   startedAt,
		CompletedAt: startedAt.Add(time.Second),
	}, CaptureBodies{}); err != nil {
		t.Fatalf("AppendCapture(no scope): %v", err)
	}
	if got := countClientKeys(t, reopened); got != 1 {
		t.Fatalf("stored %d client keys, want only the scope of the first capture", got)
	}
}

func TestSQLiteAppendCaptureValidation(t *testing.T) {
	ctx := context.Background()
	store, _ := openTempSQLite(t)
	at := time.Date(2026, 7, 8, 9, 10, 11, 0, time.UTC)
	valid := captureAt("sess", "req", "keyid", "sk-secret-value-0123456789", "m", "", at)

	cases := map[string]Capture{
		"no session":      {RequestID: valid.RequestID, StartedAt: at, CompletedAt: at.Add(time.Second)},
		"no started at":   {SessionID: valid.SessionID, RequestID: valid.RequestID, CompletedAt: at},
		"no completed at": {SessionID: valid.SessionID, RequestID: valid.RequestID, StartedAt: at},
		"completed before": {SessionID: valid.SessionID, RequestID: valid.RequestID, StartedAt: at,
			CompletedAt: at.Add(-time.Second)},
		"negative count": {SessionID: valid.SessionID, RequestID: valid.RequestID, StartedAt: at, CompletedAt: at, EventCount: -1},
	}
	for name, c := range cases {
		err := store.AppendCapture(ctx, c, CaptureBodies{})
		if err == nil {
			t.Errorf("AppendCapture(%s) succeeded, want validation error", name)
			continue
		}
		if strings.Contains(err.Error(), "sk-secret-value") {
			t.Errorf("AppendCapture(%s) error leaks the client key: %v", name, err)
		}
	}
	if got := countCaptures(t, store); got != 0 {
		t.Fatalf("rejected captures stored %d rows, want 0", got)
	}
	if got := countBodies(t, store); got != 0 {
		t.Fatalf("rejected captures stored %d body rows, want 0", got)
	}

	// A capture with no bodies is allowed and stores an empty body row.
	if err := store.AppendCapture(ctx, valid, CaptureBodies{}); err != nil {
		t.Fatalf("AppendCapture(no bodies): %v", err)
	}
	detail, err := store.RecordDetail(ctx, "sess", "req")
	if err != nil {
		t.Fatalf("RecordDetail: %v", err)
	}
	if detail.Record.EventCount != 0 || len(detail.Bodies.RequestBody) != 0 || len(detail.Bodies.WebSocketBody) != 0 {
		t.Fatalf("empty capture = %+v with bodies %+v", detail.Record, detail.Bodies)
	}
	if got := countBodies(t, store); got != 1 {
		t.Fatalf("stored %d body rows, want 1", got)
	}
	if err := store.AppendCapture(ctx, valid, CaptureBodies{}); err == nil {
		t.Fatal("AppendCapture with a duplicate (session_id, request_id) succeeded, want constraint error")
	}
}

func TestSQLiteAppendCaptureRollsBackAsOneUnit(t *testing.T) {
	ctx := context.Background()
	store, _ := openTempSQLite(t)
	at := time.Date(2026, 8, 9, 10, 11, 12, 0, time.UTC)

	const (
		scopeA = "aa11bb22cc33dd44ee55ff6600112233445566778899aabbccddeeff00112233"
		scopeB = "bb11bb22cc33dd44ee55ff6600112233445566778899aabbccddeeff00112233"
		keyA   = "sk-first-key-0123456789"
		keyB   = "sk-second-key-9876543210"
	)
	first := captureAt("sess", "req", scopeA, keyA, "m", "", at)
	if err := store.AppendCapture(ctx, first, CaptureBodies{RequestBody: []byte("keep-me")}); err != nil {
		t.Fatalf("AppendCapture(first): %v", err)
	}

	// A duplicate summary aborts the whole append: no body row and no key row of the
	// rejected capture may survive.
	duplicate := captureAt("sess", "req", scopeB, keyB, "other", "", at.Add(time.Second))
	err := store.AppendCapture(ctx, duplicate, CaptureBodies{RequestBody: []byte("retry-must-not-win")})
	if err == nil {
		t.Fatal("AppendCapture with a duplicate summary succeeded, want constraint error")
	}
	if got := countCaptures(t, store); got != 1 {
		t.Fatalf("failed append stored %d captures, want 1", got)
	}
	if got := countBodies(t, store); got != 1 {
		t.Fatalf("failed append stored %d body rows, want 1", got)
	}
	if got := countClientKeys(t, store); got != 1 {
		t.Fatalf("failed append stored %d client keys, want 1", got)
	}
	detail, err := store.RecordDetail(ctx, "sess", "req")
	if err != nil {
		t.Fatalf("RecordDetail: %v", err)
	}
	if detail.Record.Model != "m" || string(detail.Bodies.RequestBody) != "keep-me" {
		t.Fatalf("the stored capture changed: %+v %q", detail.Record, detail.Bodies.RequestBody)
	}
	var rejected int
	if err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM client_keys WHERE key_id = ?", scopeB).Scan(&rejected); err != nil {
		t.Fatalf("count rejected scope: %v", err)
	}
	if rejected != 0 {
		t.Fatalf("the rolled back append wrote %d key rows of the rejected scope", rejected)
	}

	// The same scope and key are written once the summary no longer collides.
	if err := store.AppendCapture(ctx, captureAt("sess", "req-2", scopeB, keyB, "m", "", at.Add(2*time.Second)), CaptureBodies{}); err != nil {
		t.Fatalf("AppendCapture(second): %v", err)
	}
	if got := countClientKeys(t, store); got != 2 {
		t.Fatalf("stored %d client keys, want 2", got)
	}
	if got := storedClientKey(t, store, scopeA); got != keyA {
		t.Fatalf("scope A key = %q, want the original", got)
	}
	if got := storedClientKey(t, store, scopeB); got != keyB {
		t.Fatalf("scope B key = %q, want the appended key", got)
	}
}

func TestSQLiteSearchRecordsFiltersPaginationAndOrder(t *testing.T) {
	ctx := context.Background()
	store, _ := openTempSQLite(t)

	const (
		keyA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		keyB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	base := time.Date(2026, 6, 7, 8, 9, 10, 0, time.UTC)
	seed := []Capture{
		captureAt("sess-a", "req-1", keyA, "sk-key-a-0123456789", "gpt-5", "gpt-5", base),
		captureAt("sess-a", "req-2", keyA, "sk-key-a-0123456789", "claude-sonnet", "claude-sonnet-4", base.Add(time.Second)),
		captureAt("sess-b", "req-1", keyB, "sk-key-b-9876543210", "gpt-5", "gpt-5-mini", base.Add(2*time.Second)),
		captureAt("sess-c", "req-1", "", "", "gemini-pro", "", base.Add(3*time.Second)),
		captureAt("sess-d", "req-1", keyA, "sk-key-a-0123456789", "", "", base.Add(4*time.Second)),
	}
	for i, c := range seed {
		if err := store.AppendCapture(ctx, c, CaptureBodies{RequestBody: []byte(fmt.Sprintf("body-%d", i))}); err != nil {
			t.Fatalf("AppendCapture(%s/%s): %v", c.SessionID, c.RequestID, err)
		}
	}

	all, err := store.SearchRecords(ctx, RecordFilter{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("SearchRecords(all): %v", err)
	}
	if all.Total != 5 || len(all.Items) != 5 {
		t.Fatalf("all: total %d, %d items, want 5/5", all.Total, len(all.Items))
	}
	if all.Page != 1 || all.PageSize != 10 {
		t.Fatalf("page echo = %d/%d", all.Page, all.PageSize)
	}
	wantOrder := []string{"sess-d/req-1", "sess-c/req-1", "sess-b/req-1", "sess-a/req-2", "sess-a/req-1"}
	for i, want := range wantOrder {
		if got := all.Items[i].SessionID + "/" + all.Items[i].RequestID; got != want {
			t.Fatalf("order[%d] = %s, want %s (newest first)", i, got, want)
		}
	}

	// Stable tie-break: identical started_at must order by session_id DESC then
	// request_id DESC, and paging across the boundary must not repeat or skip.
	tie := base.Add(10 * time.Second)
	for _, pair := range [][2]string{{"sess-tie", "req-a"}, {"sess-tie", "req-b"}, {"sess-tie-2", "req-a"}} {
		c := captureAt(pair[0], pair[1], keyB, "sk-key-b-9876543210", "gpt-5", "", tie)
		if err := store.AppendCapture(ctx, c, CaptureBodies{}); err != nil {
			t.Fatalf("AppendCapture(tie %v): %v", pair, err)
		}
	}
	page1, err := store.SearchRecords(ctx, RecordFilter{Page: 1, PageSize: 2})
	if err != nil {
		t.Fatalf("SearchRecords(page 1): %v", err)
	}
	page2, err := store.SearchRecords(ctx, RecordFilter{Page: 2, PageSize: 2})
	if err != nil {
		t.Fatalf("SearchRecords(page 2): %v", err)
	}
	page3, err := store.SearchRecords(ctx, RecordFilter{Page: 3, PageSize: 2})
	if err != nil {
		t.Fatalf("SearchRecords(page 3): %v", err)
	}
	page4, err := store.SearchRecords(ctx, RecordFilter{Page: 4, PageSize: 2})
	if err != nil {
		t.Fatalf("SearchRecords(page 4): %v", err)
	}
	if page1.Total != 8 || page2.Total != 8 || page3.Total != 8 || page4.Total != 8 {
		t.Fatalf("total must be the full match count: %d %d %d %d", page1.Total, page2.Total, page3.Total, page4.Total)
	}
	var paged []string
	for _, page := range []RecordPage{page1, page2, page3, page4} {
		for _, item := range page.Items {
			paged = append(paged, item.SessionID+"/"+item.RequestID)
		}
	}
	if len(paged) != 8 {
		t.Fatalf("paging returned %d records, want 8: %v", len(paged), paged)
	}
	for i := 1; i < len(paged); i++ {
		if paged[i-1] == paged[i] {
			t.Fatalf("paging repeated %s", paged[i])
		}
	}
	wantTie := []string{"sess-tie-2/req-a", "sess-tie/req-b", "sess-tie/req-a"}
	for i, want := range wantTie {
		if paged[i] != want {
			t.Fatalf("tie order[%d] = %s, want %s", i, paged[i], want)
		}
	}
	if len(page4.Items) != 2 {
		t.Fatalf("page 4 returned %d items, want 2", len(page4.Items))
	}
	past, err := store.SearchRecords(ctx, RecordFilter{Page: 5, PageSize: 2})
	if err != nil {
		t.Fatalf("SearchRecords(page 5): %v", err)
	}
	if len(past.Items) != 0 || past.Total != 8 {
		t.Fatalf("page past the end returned %d items (total %d), want 0 items and the full total", len(past.Items), past.Total)
	}

	// Key filter: exact match, and "unknown" for records without a key ID.
	byKey, err := store.SearchRecords(ctx, RecordFilter{KeyID: keyA, Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("SearchRecords(key A): %v", err)
	}
	if byKey.Total != 3 || len(byKey.Items) != 3 {
		t.Fatalf("key A matched %d records, want 3", byKey.Total)
	}
	for _, item := range byKey.Items {
		if item.ClientKeyID != keyA {
			t.Fatalf("key A filter returned client_key_id %q", item.ClientKeyID)
		}
	}
	unknown, err := store.SearchRecords(ctx, RecordFilter{KeyID: UnknownKeyID, Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("SearchRecords(unknown key): %v", err)
	}
	if unknown.Total != 1 || len(unknown.Items) != 1 || unknown.Items[0].SessionID != "sess-c" {
		t.Fatalf("unknown key matched %+v, want sess-c/req-1 only", unknown.Items)
	}

	// Model filter matches model OR requested_model, exactly.
	for _, tc := range []struct {
		model string
		want  []string
	}{
		{"gpt-5", []string{"sess-tie-2/req-a", "sess-tie/req-b", "sess-tie/req-a", "sess-b/req-1", "sess-a/req-1"}},
		{"gpt-5-mini", []string{"sess-b/req-1"}},
		{"claude-sonnet-4", []string{"sess-a/req-2"}},
		{"gpt", nil},
	} {
		got, err := store.SearchRecords(ctx, RecordFilter{Model: tc.model, Page: 1, PageSize: 10})
		if err != nil {
			t.Fatalf("SearchRecords(model %s): %v", tc.model, err)
		}
		if int(got.Total) != len(tc.want) {
			t.Fatalf("model %q matched %d records, want %d", tc.model, got.Total, len(tc.want))
		}
		for i, want := range tc.want {
			if item := got.Items[i].SessionID + "/" + got.Items[i].RequestID; item != want {
				t.Fatalf("model %q item %d = %s, want %s", tc.model, i, item, want)
			}
		}
	}

	// Time range: from inclusive, to exclusive, on started_at.
	for _, tc := range []struct {
		name     string
		from, to time.Time
		want     int64
	}{
		{"from only, inclusive", base.Add(time.Second), time.Time{}, 7},
		{"to only, exclusive", time.Time{}, base.Add(time.Second), 1},
		{"closed range", base.Add(time.Second), base.Add(3 * time.Second), 2},
		{"empty range", base.Add(20 * time.Second), base.Add(30 * time.Second), 0},
	} {
		got, err := store.SearchRecords(ctx, RecordFilter{From: tc.from, To: tc.to, Page: 1, PageSize: 10})
		if err != nil {
			t.Fatalf("SearchRecords(%s): %v", tc.name, err)
		}
		if got.Total != tc.want {
			t.Fatalf("%s matched %d records, want %d", tc.name, got.Total, tc.want)
		}
	}

	// Combined filter: time + key + model.
	combined, err := store.SearchRecords(ctx, RecordFilter{
		From: base.Add(time.Second), To: base.Add(3 * time.Second), KeyID: keyA, Model: "gpt-5", Page: 1, PageSize: 10,
	})
	if err != nil {
		t.Fatalf("SearchRecords(combined): %v", err)
	}
	if combined.Total != 0 {
		t.Fatalf("combined filter matched %d records, want 0", combined.Total)
	}
	combined, err = store.SearchRecords(ctx, RecordFilter{
		From: base, To: base.Add(3 * time.Second), KeyID: keyA, Model: "claude-sonnet", Page: 1, PageSize: 10,
	})
	if err != nil {
		t.Fatalf("SearchRecords(combined 2): %v", err)
	}
	if combined.Total != 1 || combined.Items[0].RequestID != "req-2" {
		t.Fatalf("combined filter matched %+v", combined.Items)
	}
}

func TestSQLiteSearchRecordsValidation(t *testing.T) {
	ctx := context.Background()
	store, _ := openTempSQLite(t)
	now := time.Now().UTC()

	cases := map[string]RecordFilter{
		"page zero":        {Page: 0, PageSize: 10},
		"page negative":    {Page: -1, PageSize: 10},
		"page size zero":   {Page: 1, PageSize: 0},
		"page size over":   {Page: 1, PageSize: MaxPageSize + 1},
		"page size huge":   {Page: 1, PageSize: 100000},
		"from after to":    {From: now.Add(time.Hour), To: now, Page: 1, PageSize: 10},
		"from equal to to": {From: now, To: now, Page: 1, PageSize: 10},
	}
	for name, filter := range cases {
		if _, err := store.SearchRecords(ctx, filter); err == nil {
			t.Errorf("SearchRecords(%s) succeeded, want validation error", name)
		}
	}
	for _, size := range []int{1, MaxPageSize} {
		if _, err := store.SearchRecords(ctx, RecordFilter{Page: 1, PageSize: size}); err != nil {
			t.Errorf("SearchRecords(page size %d): %v", size, err)
		}
	}
	// A filter that only sets To is valid: the zero From means "unbounded".
	if _, err := store.SearchRecords(ctx, RecordFilter{To: now, Page: 1, PageSize: 10}); err != nil {
		t.Errorf("SearchRecords(to only): %v", err)
	}
	if _, err := store.RecordDetail(ctx, "  ", "x"); err == nil {
		t.Fatal("RecordDetail with a blank session ID succeeded, want error")
	}
}

func TestSQLiteRecordOptionsMasksKeysAndListsModels(t *testing.T) {
	ctx := context.Background()
	store, _ := openTempSQLite(t)
	base := time.Date(2026, 9, 10, 11, 12, 13, 0, time.UTC)

	const (
		longKey   = "sk-live-abcdefghijklmnopqrstuvwxyz"
		mediumKey = "sk-med-0123456789"
		shortKey  = "sk-12345"
	)
	seed := []Capture{
		captureAt("s1", "r1", "id-long", "", "gpt-5", "gpt-5-mini", base),
		captureAt("s2", "r1", "id-medium", "", "claude-sonnet-4", "", base.Add(time.Second)),
		captureAt("s3", "r1", "id-short", "", "gemini-pro", "gemini-pro-exp", base.Add(2*time.Second)),
		captureAt("s4", "r1", "unknown", "", "gpt-5", "", base.Add(3*time.Second)),
	}
	for _, c := range seed {
		if err := store.AppendCapture(ctx, c, CaptureBodies{}); err != nil {
			t.Fatalf("AppendCapture(%s): %v", c.SessionID, err)
		}
	}
	// The plugin sends the literal "unknown" when the host exposes no key metadata:
	// it must land in the unknown group and never create a key row, even when a
	// plaintext is supplied by mistake.
	if err := store.AppendCapture(ctx, captureAt("s5", "r1", UnknownKeyID, "sk-ignored-0123456789", "gpt-5", "", base.Add(4*time.Second)), CaptureBodies{}); err != nil {
		t.Fatalf("AppendCapture(unknown scope): %v", err)
	}

	for scope, key := range map[string]string{"id-long": longKey, "id-medium": mediumKey, "id-short": shortKey} {
		if err := store.UpsertClientKey(ctx, scope, key); err != nil {
			t.Fatalf("UpsertClientKey(%s): %v", scope, err)
		}
	}
	if got := countClientKeys(t, store); got != 3 {
		t.Fatalf("stored %d client keys, want 3 (the unknown group must not be written)", got)
	}

	options, err := store.RecordOptions(ctx)
	if err != nil {
		t.Fatalf("RecordOptions: %v", err)
	}
	if len(options.Keys) != 4 {
		t.Fatalf("got %d key options, want 4: %+v", len(options.Keys), options.Keys)
	}
	labels := map[string]string{}
	for _, k := range options.Keys {
		labels[k.ID] = k.Label
	}
	for _, plaintext := range []string{longKey, mediumKey, shortKey} {
		for id, label := range labels {
			if strings.Contains(label, plaintext) || label == plaintext {
				t.Fatalf("label of %s leaks the key %q: %q", id, plaintext, label)
			}
		}
	}
	if got := labels["id-short"]; got == shortKey || !strings.Contains(got, "id-short") {
		t.Fatalf("short key label = %q, want a fully masked label carrying the short ID", got)
	}
	if got := labels["id-long"]; !strings.Contains(got, "sk-li") || !strings.Contains(got, "xyz") || !strings.Contains(got, "id-long") {
		t.Fatalf("long key label = %q, want masked key plus short ID", got)
	}
	if got := labels[UnknownKeyID]; got != "unknown" {
		t.Fatalf("unknown key label = %q, want %q", got, "unknown")
	}
	unknownRecords, err := store.SearchRecords(ctx, RecordFilter{KeyID: UnknownKeyID, Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("SearchRecords(unknown): %v", err)
	}
	if unknownRecords.Total != 2 || len(unknownRecords.Items) != 2 {
		t.Fatalf("unknown key filter matched %d records, want the two scope-less captures", unknownRecords.Total)
	}
	for _, item := range unknownRecords.Items {
		if item.ClientKeyID != "" {
			t.Fatalf("stored client_key_id = %q, want the canonical empty value", item.ClientKeyID)
		}
	}
	wantModels := []string{"claude-sonnet-4", "gemini-pro", "gemini-pro-exp", "gpt-5", "gpt-5-mini"}
	if len(options.Models) != len(wantModels) {
		t.Fatalf("models = %v, want %v", options.Models, wantModels)
	}
	for i, want := range wantModels {
		if options.Models[i] != want {
			t.Fatalf("models = %v, want %v", options.Models, wantModels)
		}
	}

	encoded, err := json.Marshal(options)
	if err != nil {
		t.Fatalf("marshal options: %v", err)
	}
	// The full keys must be absent, and so must their middles: only a short prefix
	// and suffix may appear in a label.
	for _, secret := range []string{
		longKey, mediumKey, shortKey,
		longKey[5 : len(longKey)-3],
		mediumKey[5 : len(mediumKey)-3],
	} {
		if bytes.Contains(encoded, []byte(secret)) {
			t.Fatalf("RecordOptions JSON leaks %q: %s", secret, encoded)
		}
	}
	if bytes.Contains(encoded, []byte("client_key")) {
		t.Fatalf("RecordOptions JSON mentions client_key: %s", encoded)
	}
}

func TestSQLiteRecordJSONNeverExposesClientKey(t *testing.T) {
	ctx := context.Background()
	store, _ := openTempSQLite(t)
	at := time.Date(2026, 10, 11, 12, 13, 14, 0, time.UTC)
	const key = "sk-live-ZZZZ0123456789abcdefghijkl"
	capture := captureAt("sess-json", "req-json", "id-json", key, "gpt-5", "gpt-5-mini", at)
	if err := store.AppendCapture(ctx, capture, CaptureBodies{RequestBody: binaryPayload(), ResponseBody: []byte("resp")}); err != nil {
		t.Fatalf("AppendCapture: %v", err)
	}

	page, err := store.SearchRecords(ctx, RecordFilter{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("SearchRecords: %v", err)
	}
	detail, err := store.RecordDetail(ctx, "sess-json", "req-json")
	if err != nil {
		t.Fatalf("RecordDetail: %v", err)
	}
	// The list joins the key table, so the in-memory row carries the plaintext; it
	// must still never reach any JSON representation.
	if page.Items[0].ClientKey != key {
		t.Fatalf("list did not join the stored key: %q", page.Items[0].ClientKey)
	}
	if detail.Record.ClientKey != "" {
		t.Fatalf("detail carries the plaintext key: %q", detail.Record.ClientKey)
	}
	for name, value := range map[string]any{
		"Capture":      page.Items[0],
		"RecordPage":   page,
		"RecordDetail": detail,
	} {
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("marshal %s: %v", name, err)
		}
		if bytes.Contains(encoded, []byte(key)) || bytes.Contains(encoded, []byte("ZZZZ0123456789")) {
			t.Fatalf("%s JSON leaks the client key: %s", name, encoded)
		}
		if bytes.Contains(encoded, []byte(`"client_key":`)) {
			t.Fatalf("%s JSON contains a client_key field: %s", name, encoded)
		}
	}
	if page.Items[0].ClientKeyID != "id-json" || detail.Record.Model != "gpt-5" {
		t.Fatalf("unexpected record: %+v / %+v", page.Items[0], detail.Record)
	}
}

func TestCaptureBodiesJSONTagsAreSnakeCaseWithBase64(t *testing.T) {
	request := binaryPayload()
	bodies := CaptureBodies{RequestBody: request, ResponseBody: []byte("response"), WebSocketBody: []byte{0x01, 0x02}}

	encoded, err := json.Marshal(bodies)
	if err != nil {
		t.Fatalf("marshal bodies: %v", err)
	}
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal bodies: %v", err)
	}
	for _, field := range []string{"request_body", "response_body", "websocket_body"} {
		if _, ok := decoded[field]; !ok {
			t.Fatalf("bodies JSON is missing %q: %s", field, encoded)
		}
	}
	var gotRequest string
	if err := json.Unmarshal(decoded["request_body"], &gotRequest); err != nil {
		t.Fatalf("request_body is not a JSON string: %v", err)
	}
	if want := base64.StdEncoding.EncodeToString(request); gotRequest != want {
		t.Fatalf("request_body base64 = %q, want %q", gotRequest, want)
	}
}

func TestMaskClientKeyKeepsKeysPrivate(t *testing.T) {
	cases := map[string]string{
		"":                                   "",
		"k":                                  "\u2026",
		"sk-12345":                           "\u2026",
		"sk-123456":                          "sk-1\u202656",
		"sk-1234567":                         "sk-1\u202667",
		"sk-12345678":                        "sk-1\u202678",
		"sk-123456789":                       "sk-1\u202689",
		"sk-med-0123456789":                  "sk-me\u2026789",
		"sk-live-abcdefghijklmnopqrstuvwxyz": "sk-li\u2026xyz",
	}
	for key, want := range cases {
		got := maskClientKey(key)
		if got != want {
			t.Errorf("maskClientKey(%q) = %q, want %q", key, got, want)
		}
		if key != "" && got == key {
			t.Errorf("maskClientKey(%q) returned the key unchanged", key)
		}
	}
	// A label must never equal the key it describes, whatever the length.
	for _, key := range []string{"k", "sk-12345678", "sk-123456789", "sk-med-0123456789", "sk-live-abcdefghijklmnopqrstuvwxyz"} {
		if label := keyLabel("id-long", key); label == key {
			t.Errorf("keyLabel(%q) returned the key itself", key)
		}
	}
	if got := keyLabel("", ""); got != "" {
		t.Errorf("keyLabel(empty) = %q, want empty", got)
	}
}

func TestSQLiteUpsertClientKeyFillsOnlyTheScope(t *testing.T) {
	ctx := context.Background()
	store, _ := openTempSQLite(t)

	const (
		scopeA = "aa11bb22cc33dd44ee55ff6600112233445566778899aabbccddeeff00112233"
		scopeB = "bb11bb22cc33dd44ee55ff6600112233445566778899aabbccddeeff00112233"
		scopeC = "cc11bb22cc33dd44ee55ff6600112233445566778899aabbccddeeff00112233"
		keyA   = "sk-resolved-scope-a-0123456789"
		keyC   = "sk-known-scope-c-9876543210"
	)
	base := time.Date(2027, 1, 2, 3, 4, 5, 0, time.UTC)
	// Three captures of scope A and one of scope B were stored while the plaintext
	// was still unknown; scope C already had it; one record has no scope at all.
	seed := []Capture{
		captureAt("s-a1", "r1", scopeA, "", "gpt-5", "", base),
		captureAt("s-a2", "r1", scopeA, "", "gpt-5", "", base.Add(time.Second)),
		captureAt("s-a3", "r1", scopeA, "", "claude-sonnet", "", base.Add(2*time.Second)),
		captureAt("s-b1", "r1", scopeB, "", "gpt-5", "", base.Add(3*time.Second)),
		captureAt("s-c1", "r1", scopeC, keyC, "gpt-5", "", base.Add(4*time.Second)),
		captureAt("s-u1", "r1", "unknown", "", "gpt-5", "", base.Add(5*time.Second)),
	}
	for _, c := range seed {
		if err := store.AppendCapture(ctx, c, CaptureBodies{}); err != nil {
			t.Fatalf("AppendCapture(%s): %v", c.SessionID, err)
		}
	}
	before := countCaptures(t, store)

	// Before the upsert, scope A is real but unresolved: it must not be advertised
	// as the unknown key.
	options, err := store.RecordOptions(ctx)
	if err != nil {
		t.Fatalf("RecordOptions: %v", err)
	}
	labels := map[string]string{}
	for _, k := range options.Keys {
		labels[k.ID] = k.Label
	}
	unresolved := labels[scopeA]
	if unresolved != unresolvedKeyText+" \u00b7 "+scopeA[:8] {
		t.Fatalf("unresolved label = %q, want the unresolved text plus the short scope", unresolved)
	}
	if unresolved == labels[UnknownKeyID] || strings.Contains(unresolved, "unknown") {
		t.Fatalf("unresolved scope %q is presented as the unknown key (label %q)", scopeA, unresolved)
	}
	if labels[scopeC] != maskClientKey(keyC)+" \u00b7 "+scopeC[:8] {
		t.Fatalf("resolved scope C label = %q", labels[scopeC])
	}
	if labels[UnknownKeyID] != "unknown" || labels[scopeB] == "" {
		t.Fatalf("options = %+v, want a separate unknown option and a scope B option", options.Keys)
	}

	if err := store.UpsertClientKey(ctx, scopeA, keyA); err != nil {
		t.Fatalf("UpsertClientKey(scope A): %v", err)
	}
	// Only scope A changed, and no capture row was inserted or deleted.
	if got := storedClientKey(t, store, scopeA); got != keyA {
		t.Fatalf("scope A key = %q, want the upserted key", got)
	}
	if after := countCaptures(t, store); after != before {
		t.Fatalf("UpsertClientKey changed the capture count: %d -> %d (it must never insert)", before, after)
	}
	var emptyScopeB int
	if err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM client_keys WHERE key_id = ?", scopeB).Scan(&emptyScopeB); err != nil {
		t.Fatalf("count scope B keys: %v", err)
	}
	if emptyScopeB != 0 {
		t.Fatalf("scope B gained %d key rows, want 0", emptyScopeB)
	}

	// The label now masks the upserted key and never shows it in full.
	options, err = store.RecordOptions(ctx)
	if err != nil {
		t.Fatalf("RecordOptions after upsert: %v", err)
	}
	labels = map[string]string{}
	for _, k := range options.Keys {
		labels[k.ID] = k.Label
	}
	if got := labels[scopeA]; got == keyA || !strings.Contains(got, keyA[:5]) || !strings.Contains(got, scopeA[:8]) {
		t.Fatalf("resolved scope A label = %q, want a masked key plus the short scope", got)
	}
	if got := labels[scopeB]; got != unresolvedKeyText+" \u00b7 "+scopeB[:8] {
		t.Fatalf("scope B label = %q, want it still unresolved", got)
	}
	encoded, err := json.Marshal(options)
	if err != nil {
		t.Fatalf("marshal options: %v", err)
	}
	for _, secret := range []string{keyA, keyC, keyA[5 : len(keyA)-3]} {
		if bytes.Contains(encoded, []byte(secret)) {
			t.Fatalf("RecordOptions JSON leaks %q: %s", secret, encoded)
		}
	}
	if bytes.Contains(encoded, []byte(`"client_key":`)) {
		t.Fatalf("RecordOptions JSON contains a client_key field: %s", encoded)
	}

	// Records of the scope still hide the key, in the list and in detail.
	page, err := store.SearchRecords(ctx, RecordFilter{KeyID: scopeA, Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("SearchRecords(scope A): %v", err)
	}
	if page.Total != 3 {
		t.Fatalf("scope A matched %d records, want 3", page.Total)
	}
	detail, err := store.RecordDetail(ctx, "s-a1", "r1")
	if err != nil {
		t.Fatalf("RecordDetail: %v", err)
	}
	if detail.Record.ClientKey != "" {
		t.Fatalf("RecordDetail returned the plaintext key %q", detail.Record.ClientKey)
	}
	for name, value := range map[string]any{"RecordPage": page, "RecordDetail": detail} {
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("marshal %s: %v", name, err)
		}
		if bytes.Contains(encoded, []byte(keyA)) {
			t.Fatalf("%s JSON leaks the resolved key: %s", name, encoded)
		}
	}

	// Upserting a scope with no captures never touches the capture table.
	if err := store.UpsertClientKey(ctx, "ff11bb22cc33dd44ee55ff6600112233445566778899aabbccddeeff00112233", keyA); err != nil {
		t.Fatalf("UpsertClientKey(unused scope): %v", err)
	}
	if after := countCaptures(t, store); after != before {
		t.Fatalf("upserting an unused scope inserted captures: %d -> %d", before, after)
	}
	keysBeforeRejects := countClientKeys(t, store)
	for _, bad := range []struct {
		name       string
		keyID, key string
	}{
		{"empty scope", "", keyA},
		{"unknown scope", UnknownKeyID, keyA},
		{"blank scope", "   ", keyA},
		{"empty key", scopeB, ""},
		{"blank key", scopeB, "   "},
	} {
		if err := store.UpsertClientKey(ctx, bad.keyID, bad.key); err == nil {
			t.Errorf("UpsertClientKey(%s) succeeded, want validation error", bad.name)
		}
	}
	if got := countClientKeys(t, store); got != keysBeforeRejects {
		t.Fatalf("a rejected upsert changed the key table: %d rows, want %d", got, keysBeforeRejects)
	}
	if err := store.UpsertClientKey(ctx, scopeA, keyA); err != nil {
		t.Fatalf("UpsertClientKey(scope A) twice: %v", err)
	}
}

// TestSQLiteUpsertClientKeyUpdatesOnlyWhenDifferentAndNeverRewritesCaptures pins the
// two invariants of the key table: a repeated upsert with the same plaintext is a
// no-op, and a changed plaintext only rewrites client_keys — the historical capture
// rows keep every column they had.
func TestSQLiteUpsertClientKeyUpdatesOnlyWhenDifferentAndNeverRewritesCaptures(t *testing.T) {
	ctx := context.Background()
	store, _ := openTempSQLite(t)

	const (
		scope = "11223344556677889900aabbccddeeff11223344556677889900aabbccddeeff"
		key1  = "sk-first-key-0123456789"
		key2  = "sk-second-key-9876543210"
	)
	at := time.Date(2027, 5, 6, 7, 8, 9, 0, time.UTC)
	if err := store.AppendCapture(ctx, captureAt("sess", "req", scope, key1, "gpt-5", "gpt-5-mini", at), CaptureBodies{RequestBody: []byte("body")}); err != nil {
		t.Fatalf("AppendCapture: %v", err)
	}

	snapshot := func() string {
		t.Helper()
		var row string
		if err := store.db.QueryRowContext(ctx,
			"SELECT client_key_id || '|' || model || '|' || requested_model || '|' || event_count || '|' || started_at_us || '|' || completed_at_us FROM captures WHERE session_id = ? AND request_id = ?",
			"sess", "req").Scan(&row); err != nil {
			t.Fatalf("snapshot capture: %v", err)
		}
		return row
	}
	before := snapshot()

	if err := store.UpsertClientKey(ctx, scope, key1); err != nil {
		t.Fatalf("UpsertClientKey(same key): %v", err)
	}
	if got := countClientKeys(t, store); got != 1 {
		t.Fatalf("stored %d key rows after a repeated upsert, want 1", got)
	}
	if got := storedClientKey(t, store, scope); got != key1 {
		t.Fatalf("key after repeated upsert = %q, want the original", got)
	}

	if err := store.UpsertClientKey(ctx, scope, key2); err != nil {
		t.Fatalf("UpsertClientKey(new key): %v", err)
	}
	if got := countClientKeys(t, store); got != 1 {
		t.Fatalf("changing the key created %d rows, want 1", got)
	}
	if got := storedClientKey(t, store, scope); got != key2 {
		t.Fatalf("key after upsert = %q, want the new key", got)
	}
	if after := snapshot(); after != before {
		t.Fatalf("upserting a key rewrote the historical capture:\n before %s\n after  %s", before, after)
	}
	detail, err := store.RecordDetail(ctx, "sess", "req")
	if err != nil {
		t.Fatalf("RecordDetail: %v", err)
	}
	if detail.Record.Model != "gpt-5" || detail.Record.RequestedModel != "gpt-5-mini" || string(detail.Bodies.RequestBody) != "body" {
		t.Fatalf("the capture changed after a key upsert: %+v %q", detail.Record, detail.Bodies.RequestBody)
	}
	// The list joins the current plaintext, so the masked label follows the update.
	options, err := store.RecordOptions(ctx)
	if err != nil {
		t.Fatalf("RecordOptions: %v", err)
	}
	for _, k := range options.Keys {
		if k.ID == scope && (k.Label == key2 || !strings.Contains(k.Label, key2[:5])) {
			t.Fatalf("scope label = %q, want the masked new key", k.Label)
		}
	}
}
