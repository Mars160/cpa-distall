package storage

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"
)

// captureAt builds a summary whose timestamps are microsecond-aligned, the
// precision the captures table stores.
func captureAt(captureID, keyID, key, model, requested string, startedAt time.Time) Capture {
	return Capture{
		CaptureID:      captureID,
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
	store, dir := openTempSQLite(t)

	const (
		keyID = "3f1c9b0e5d2a4c6f8b1e3d5a7c9f0b2d4e6a8c0f1b3d5e7a9c1b3d5f7a9c1b3d"
		key   = "sk-live-abcdefghijklmnopqrstuvwxyz"
	)
	captureID := testCaptureID("summary-with-bodies")
	startedAt := time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC)
	requestBody := binaryPayload()
	responseBody := []byte(`{"id":"chatcmpl-1","choices":[]}`)
	websocketBody := []byte{0x00, 0xff, 0xfe, 0x80}

	capture := captureAt(captureID, keyID, key, "gpt-5", "gpt-5-mini", startedAt)
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

	detail, err := store.RecordDetail(ctx, captureID)
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
	reopened := reopenTempSQLite(t, dir, 1)
	again, err := reopened.RecordDetail(ctx, captureID)
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
		CaptureID:   testCaptureID("no-scope"),
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
	valid := captureAt(testCaptureID("valid"), "keyid", "sk-secret-value-0123456789", "m", "", at)

	cases := map[string]Capture{
		"no capture id":    {StartedAt: at, CompletedAt: at.Add(time.Second)},
		"blank capture id": {CaptureID: "   ", StartedAt: at, CompletedAt: at.Add(time.Second)},
		"short capture id": {CaptureID: testCaptureID("short")[:30], StartedAt: at, CompletedAt: at.Add(time.Second)},
		"long capture id":  {CaptureID: testCaptureID("long") + "ab", StartedAt: at, CompletedAt: at.Add(time.Second)},
		"non hex capture id": {CaptureID: strings.Repeat("z", CaptureIDHexLen), StartedAt: at,
			CompletedAt: at.Add(time.Second)},
		"no started at":   {CaptureID: valid.CaptureID, CompletedAt: at},
		"no completed at": {CaptureID: valid.CaptureID, StartedAt: at},
		"completed before": {CaptureID: valid.CaptureID, StartedAt: at,
			CompletedAt: at.Add(-time.Second)},
		"negative count": {CaptureID: valid.CaptureID, StartedAt: at, CompletedAt: at, EventCount: -1},
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
	// An invalid ID must never reach a query: the detail lookup rejects it too.
	if _, err := store.RecordDetail(ctx, "not-a-capture-id"); err == nil {
		t.Fatal("RecordDetail(invalid ID) succeeded, want error")
	}
	if _, err := store.GetRecord(ctx, testCaptureID("valid")[:16]); err == nil {
		t.Fatal("GetRecord(short ID) succeeded, want error")
	}

	// A capture with no bodies is allowed and stores an empty body row.
	if err := store.AppendCapture(ctx, valid, CaptureBodies{}); err != nil {
		t.Fatalf("AppendCapture(no bodies): %v", err)
	}
	detail, err := store.RecordDetail(ctx, valid.CaptureID)
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
		t.Fatal("AppendCapture with a duplicate capture ID succeeded, want constraint error")
	}
	// Uppercase hex names the same capture: the ID is normalised, so it collides.
	upper := valid
	upper.CaptureID = strings.ToUpper(valid.CaptureID)
	if err := store.AppendCapture(ctx, upper, CaptureBodies{}); err == nil {
		t.Fatal("AppendCapture(uppercase duplicate) succeeded, want constraint error")
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
	firstID := testCaptureID("rollback-first")
	first := captureAt(firstID, scopeA, keyA, "m", "", at)
	if err := store.AppendCapture(ctx, first, CaptureBodies{RequestBody: []byte("keep-me")}); err != nil {
		t.Fatalf("AppendCapture(first): %v", err)
	}

	// A duplicate summary aborts the whole append: no body row and no key row of the
	// rejected capture may survive.
	duplicate := captureAt(firstID, scopeB, keyB, "other", "", at.Add(time.Second))
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
	detail, err := store.RecordDetail(ctx, firstID)
	if err != nil {
		t.Fatalf("RecordDetail: %v", err)
	}
	if detail.Record.Model != "m" || string(detail.Bodies.RequestBody) != "keep-me" {
		t.Fatalf("the stored capture changed: %+v %q", detail.Record, detail.Bodies.RequestBody)
	}
	if _, ok := clientKeyInShard(t, store, 0, scopeB); ok {
		t.Fatal("the rolled back append wrote a key row of the rejected scope")
	}

	// The same scope and key are written once the summary no longer collides.
	if err := store.AppendCapture(ctx, captureAt(testCaptureID("rollback-second"), scopeB, keyB, "m", "", at.Add(2*time.Second)), CaptureBodies{}); err != nil {
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

// TestSQLiteSearchRecordsFiltersPaginationAndOrder pins the filter vocabulary and
// the ordering contract: started_at DESC, capture_id DESC, total = full match
// count, and a page window that never repeats or skips a record.
func TestSQLiteSearchRecordsFiltersPaginationAndOrder(t *testing.T) {
	ctx := context.Background()
	store, _ := openTempSQLite(t)

	const (
		keyA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		keyB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	base := time.Date(2026, 6, 7, 8, 9, 10, 0, time.UTC)
	labels := []string{"a1", "a2", "b1", "c1", "d1"}
	seed := []Capture{
		captureAt(testCaptureID("a1"), keyA, "sk-key-a-0123456789", "gpt-5", "gpt-5", base),
		captureAt(testCaptureID("a2"), keyA, "sk-key-a-0123456789", "claude-sonnet", "claude-sonnet-4", base.Add(time.Second)),
		captureAt(testCaptureID("b1"), keyB, "sk-key-b-9876543210", "gpt-5", "gpt-5-mini", base.Add(2*time.Second)),
		captureAt(testCaptureID("c1"), "", "", "gemini-pro", "", base.Add(3*time.Second)),
		captureAt(testCaptureID("d1"), keyA, "sk-key-a-0123456789", "", "", base.Add(4*time.Second)),
	}
	for i, c := range seed {
		if err := store.AppendCapture(ctx, c, CaptureBodies{RequestBody: []byte(fmt.Sprintf("body-%d", i))}); err != nil {
			t.Fatalf("AppendCapture(%s): %v", c.CaptureID, err)
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
	wantOrder := []string{labels[4], labels[3], labels[2], labels[1], labels[0]}
	for i, want := range wantOrder {
		if got := all.Items[i].CaptureID; got != testCaptureID(want) {
			t.Fatalf("order[%d] = %s, want %s (newest first)", i, got, testCaptureID(want))
		}
	}

	// Stable tie-break: identical started_at must order by capture_id DESC, and
	// paging across the boundary must not repeat or skip.
	tie := base.Add(10 * time.Second)
	tieLabels := []string{"tie-a", "tie-b", "tie-c"}
	tieIDs := make([]string, 0, len(tieLabels))
	for _, label := range tieLabels {
		id := testCaptureID(label)
		tieIDs = append(tieIDs, id)
		c := captureAt(id, keyB, "sk-key-b-9876543210", "gpt-5", "", tie)
		if err := store.AppendCapture(ctx, c, CaptureBodies{}); err != nil {
			t.Fatalf("AppendCapture(tie %s): %v", label, err)
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(tieIDs)))

	var paged []Capture
	for page := 1; page <= 4; page++ {
		got, err := store.SearchRecords(ctx, RecordFilter{Page: page, PageSize: 2})
		if err != nil {
			t.Fatalf("SearchRecords(page %d): %v", page, err)
		}
		if got.Total != 8 {
			t.Fatalf("page %d total = %d, want the full match count 8", page, got.Total)
		}
		paged = append(paged, got.Items...)
	}
	if len(paged) != 8 {
		t.Fatalf("paging returned %d records, want 8", len(paged))
	}
	for i, want := range tieIDs {
		if paged[i].CaptureID != want {
			t.Fatalf("tie order[%d] = %s, want %s", i, paged[i].CaptureID, want)
		}
	}
	for i := 1; i < len(paged); i++ {
		if paged[i-1].CaptureID == paged[i].CaptureID {
			t.Fatalf("paging repeated %s", paged[i].CaptureID)
		}
		if paged[i].StartedAt.After(paged[i-1].StartedAt) {
			t.Fatalf("paging returned %s before %s", paged[i].StartedAt, paged[i-1].StartedAt)
		}
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
	if unknown.Total != 1 || len(unknown.Items) != 1 || unknown.Items[0].CaptureID != testCaptureID("c1") {
		t.Fatalf("unknown key matched %+v, want the scope-less capture only", unknown.Items)
	}

	// Model filter matches model OR requested_model, exactly.
	for _, tc := range []struct {
		model string
		want  []string
	}{
		{"gpt-5", []string{labels[2], labels[0]}},
		{"gpt-5-mini", []string{labels[2]}},
		{"claude-sonnet-4", []string{labels[1]}},
		{"gpt", nil},
	} {
		got, err := store.SearchRecords(ctx, RecordFilter{Model: tc.model, Page: 1, PageSize: 10})
		if err != nil {
			t.Fatalf("SearchRecords(model %s): %v", tc.model, err)
		}
		// The three tie records also carry gpt-5.
		extra := 0
		if tc.model == "gpt-5" {
			extra = 3
		}
		if int(got.Total) != len(tc.want)+extra {
			t.Fatalf("model %q matched %d records, want %d", tc.model, got.Total, len(tc.want)+extra)
		}
		for i, want := range tc.want {
			offset := 0
			if tc.model == "gpt-5" {
				offset = 3
			}
			if item := got.Items[i+offset].CaptureID; item != testCaptureID(want) {
				t.Fatalf("model %q item %d = %s, want %s", tc.model, i, item, testCaptureID(want))
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
	if combined.Total != 1 || combined.Items[0].CaptureID != testCaptureID("a2") {
		t.Fatalf("combined filter matched %+v", combined.Items)
	}
}

// TestSQLiteSearchRecordsMergesShards proves that a sharded store serves one
// globally ordered list: the records are spread over several shards, yet every
// page is a window of the same total order, with no duplicate and no gap.
func TestSQLiteSearchRecordsMergesShards(t *testing.T) {
	ctx := context.Background()
	const writerCount = 4
	store, _ := openTempSQLiteShards(t, writerCount)

	base := time.Date(2026, 11, 12, 13, 14, 15, 0, time.UTC)
	const records = 61
	for i := 0; i < records; i++ {
		id := testCaptureID(fmt.Sprintf("merge-%03d", i))
		at := base.Add(time.Duration(i) * time.Second)
		c := captureAt(id, "scope-merge", "", fmt.Sprintf("model-%d", i%3), "", at)
		if err := store.AppendCapture(ctx, c, CaptureBodies{RequestBody: []byte(fmt.Sprintf("body-%03d", i))}); err != nil {
			t.Fatalf("AppendCapture(%d): %v", i, err)
		}
	}
	if got := countCaptures(t, store); got != records {
		t.Fatalf("stored %d captures, want %d", got, records)
	}
	// Every shard must really hold records, otherwise the merge is not exercised.
	used := 0
	for _, db := range shardDBs(store) {
		var n int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM captures").Scan(&n); err != nil {
			t.Fatalf("count shard: %v", err)
		}
		if n > 0 {
			used++
		}
	}
	if used != writerCount {
		t.Fatalf("%d of %d shards hold records; the distribution is not spread", used, writerCount)
	}

	// One big page defines the expected global order.
	whole, err := store.SearchRecords(ctx, RecordFilter{Page: 1, PageSize: MaxPageSize})
	if err != nil {
		t.Fatalf("SearchRecords(whole): %v", err)
	}
	if whole.Total != records || len(whole.Items) != records {
		t.Fatalf("whole page: total %d, %d items, want %d", whole.Total, len(whole.Items), records)
	}
	for i := 1; i < len(whole.Items); i++ {
		prev, cur := whole.Items[i-1], whole.Items[i]
		if cur.StartedAt.After(prev.StartedAt) {
			t.Fatalf("order[%d] started after order[%d]", i, i-1)
		}
		if cur.StartedAt.Equal(prev.StartedAt) && cur.CaptureID > prev.CaptureID {
			t.Fatalf("tie order[%d] = %s > %s", i, cur.CaptureID, prev.CaptureID)
		}
	}

	// Paging with a small page size must reproduce exactly that order.
	const pageSize = 7
	var paged []Capture
	for page := 1; ; page++ {
		got, err := store.SearchRecords(ctx, RecordFilter{Page: page, PageSize: pageSize})
		if err != nil {
			t.Fatalf("SearchRecords(page %d): %v", page, err)
		}
		if got.Total != records {
			t.Fatalf("page %d total = %d, want %d", page, got.Total, records)
		}
		if len(got.Items) == 0 {
			break
		}
		paged = append(paged, got.Items...)
		if page > records {
			t.Fatal("paging did not terminate")
		}
	}
	if len(paged) != records {
		t.Fatalf("paging returned %d records, want %d", len(paged), records)
	}
	for i := range paged {
		if paged[i].CaptureID != whole.Items[i].CaptureID {
			t.Fatalf("paged[%d] = %s, want %s", i, paged[i].CaptureID, whole.Items[i].CaptureID)
		}
	}

	// The bodies of every record are reachable by ID alone, across shards.
	for i := 0; i < records; i++ {
		id := testCaptureID(fmt.Sprintf("merge-%03d", i))
		detail, err := store.RecordDetail(ctx, id)
		if err != nil {
			t.Fatalf("RecordDetail(%s): %v", id, err)
		}
		if want := fmt.Sprintf("body-%03d", i); string(detail.Bodies.RequestBody) != want {
			t.Fatalf("bodies of %s = %q, want %q", id, detail.Bodies.RequestBody, want)
		}
	}
}

func TestSQLiteSearchRecordsValidation(t *testing.T) {
	ctx := context.Background()
	store, _ := openTempSQLite(t)
	now := time.Now().UTC()

	cases := map[string]RecordFilter{
		"page zero":             {Page: 0, PageSize: 10},
		"page negative":         {Page: -1, PageSize: 10},
		"page size zero":        {Page: 1, PageSize: 0},
		"page size over":        {Page: 1, PageSize: MaxPageSize + 1},
		"page size huge":        {Page: 1, PageSize: 100000},
		"from after to":         {From: now.Add(time.Hour), To: now, Page: 1, PageSize: 10},
		"from equal to to":      {From: now, To: now, Page: 1, PageSize: 10},
		"page offset too large": {Page: 1 << 30, PageSize: MaxPageSize},
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
	if _, err := store.RecordDetail(ctx, "   "); err == nil {
		t.Fatal("RecordDetail with a blank capture ID succeeded, want error")
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
		captureAt(testCaptureID("o1"), "id-long", "", "gpt-5", "gpt-5-mini", base),
		captureAt(testCaptureID("o2"), "id-medium", "", "claude-sonnet-4", "", base.Add(time.Second)),
		captureAt(testCaptureID("o3"), "id-short", "", "gemini-pro", "gemini-pro-exp", base.Add(2*time.Second)),
		captureAt(testCaptureID("o4"), "unknown", "", "gpt-5", "", base.Add(3*time.Second)),
	}
	for _, c := range seed {
		if err := store.AppendCapture(ctx, c, CaptureBodies{}); err != nil {
			t.Fatalf("AppendCapture(%s): %v", c.CaptureID, err)
		}
	}
	// The plugin sends the literal "unknown" when the host exposes no key metadata:
	// it must land in the unknown group and never create a key row, even when a
	// plaintext is supplied by mistake.
	if err := store.AppendCapture(ctx, captureAt(testCaptureID("o5"), UnknownKeyID, "sk-ignored-0123456789", "gpt-5", "", base.Add(4*time.Second)), CaptureBodies{}); err != nil {
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

// TestSQLiteRecordOptionsMergesShards proves that the filter vocabulary is the
// union of the shards: a scope or model that only occurs in one shard is still
// selectable, and a key that arrives late is visible in every shard.
func TestSQLiteRecordOptionsMergesShards(t *testing.T) {
	ctx := context.Background()
	const writerCount = 4
	store, _ := openTempSQLiteShards(t, writerCount)

	const (
		lateScope = "cafe11bb22cc33dd44ee55ff6600112233445566778899aabbccddeeff001122"
		lateKey   = "sk-late-usage-key-0123456789"
	)
	base := time.Date(2026, 12, 13, 14, 15, 16, 0, time.UTC)
	const records = 40
	for i := 0; i < records; i++ {
		id := testCaptureID(fmt.Sprintf("options-%03d", i))
		scope := fmt.Sprintf("scope-%d", i%3)
		if i%7 == 0 {
			scope = lateScope
		}
		c := captureAt(id, scope, "", fmt.Sprintf("model-%d", i%4), "", base.Add(time.Duration(i)*time.Second))
		if err := store.AppendCapture(ctx, c, CaptureBodies{}); err != nil {
			t.Fatalf("AppendCapture(%d): %v", i, err)
		}
	}

	// Every scope is unresolved until the plaintext arrives, but must be listed as
	// its own option instead of being folded into the unknown group.
	options, err := store.RecordOptions(ctx)
	if err != nil {
		t.Fatalf("RecordOptions: %v", err)
	}
	ids := map[string]bool{}
	for _, k := range options.Keys {
		ids[k.ID] = true
	}
	for _, scope := range []string{"scope-0", "scope-1", "scope-2", lateScope} {
		if !ids[scope] {
			t.Fatalf("scope %s is missing from the merged options: %+v", scope, options.Keys)
		}
	}
	if ids[UnknownKeyID] {
		t.Fatalf("no capture is scope-less, yet the unknown option is offered: %+v", options.Keys)
	}
	if len(options.Models) != 4 {
		t.Fatalf("merged models = %v, want the 4 models of the shards", options.Models)
	}

	// A late usage key must reach every shard, so the merged label resolves.
	if err := store.UpsertClientKey(ctx, lateScope, lateKey); err != nil {
		t.Fatalf("UpsertClientKey(late scope): %v", err)
	}
	for i := 0; i < writerCount; i++ {
		got, ok := clientKeyInShard(t, store, i, lateScope)
		if !ok {
			t.Fatalf("shard %d has no row for the late scope", i)
		}
		if got != lateKey {
			t.Fatalf("shard %d stored %q, want %q", i, got, lateKey)
		}
	}
	options, err = store.RecordOptions(ctx)
	if err != nil {
		t.Fatalf("RecordOptions after upsert: %v", err)
	}
	resolved := false
	for _, k := range options.Keys {
		if k.ID != lateScope {
			continue
		}
		resolved = true
		if k.Label == lateKey || !strings.Contains(k.Label, lateKey[:5]) {
			t.Fatalf("late scope label = %q, want a masked key", k.Label)
		}
	}
	if !resolved {
		t.Fatalf("the late scope disappeared from the options: %+v", options.Keys)
	}
	// The list join resolves the plaintext in whichever shard holds the record.
	page, err := store.SearchRecords(ctx, RecordFilter{KeyID: lateScope, Page: 1, PageSize: MaxPageSize})
	if err != nil {
		t.Fatalf("SearchRecords(late scope): %v", err)
	}
	if page.Total == 0 {
		t.Fatal("the late scope matched no record")
	}
	for _, item := range page.Items {
		if item.ClientKey != lateKey {
			t.Fatalf("record %s did not join the late key: %q", item.CaptureID, item.ClientKey)
		}
	}
}

func TestSQLiteRecordJSONNeverExposesClientKey(t *testing.T) {
	ctx := context.Background()
	store, _ := openTempSQLite(t)
	at := time.Date(2026, 10, 11, 12, 13, 14, 0, time.UTC)
	const key = "sk-live-ZZZZ0123456789abcdefghijkl"
	captureID := testCaptureID("json")
	capture := captureAt(captureID, "id-json", key, "gpt-5", "gpt-5-mini", at)
	capture.RequestID = "host-request-verbatim-42"
	if err := store.AppendCapture(ctx, capture, CaptureBodies{RequestBody: binaryPayload(), ResponseBody: []byte("resp")}); err != nil {
		t.Fatalf("AppendCapture: %v", err)
	}

	page, err := store.SearchRecords(ctx, RecordFilter{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("SearchRecords: %v", err)
	}
	detail, err := store.RecordDetail(ctx, captureID)
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
	// The summary serialises the capture ID and the host request ID exactly as the
	// host reported it; there is no session identity and no digest that would hide
	// the host's own identifier.
	encoded, err := json.Marshal(detail.Record)
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	for _, field := range []string{`"capture_id":`, `"request_id":`} {
		if !bytes.Contains(encoded, []byte(field)) {
			t.Fatalf("record JSON is missing %s: %s", field, encoded)
		}
	}
	if !bytes.Contains(encoded, []byte(capture.RequestID)) {
		t.Fatalf("record JSON does not carry the host request ID verbatim (%q): %s", capture.RequestID, encoded)
	}
	if bytes.Contains(encoded, []byte(`"session_id":`)) {
		t.Fatalf("record JSON still exposes a session identity: %s", encoded)
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
		captureAt(testCaptureID("k-a1"), scopeA, "", "gpt-5", "", base),
		captureAt(testCaptureID("k-a2"), scopeA, "", "gpt-5", "", base.Add(time.Second)),
		captureAt(testCaptureID("k-a3"), scopeA, "", "claude-sonnet", "", base.Add(2*time.Second)),
		captureAt(testCaptureID("k-b1"), scopeB, "", "gpt-5", "", base.Add(3*time.Second)),
		captureAt(testCaptureID("k-c1"), scopeC, keyC, "gpt-5", "", base.Add(4*time.Second)),
		captureAt(testCaptureID("k-u1"), "unknown", "", "gpt-5", "", base.Add(5*time.Second)),
	}
	for _, c := range seed {
		if err := store.AppendCapture(ctx, c, CaptureBodies{}); err != nil {
			t.Fatalf("AppendCapture(%s): %v", c.CaptureID, err)
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
	if _, ok := clientKeyInShard(t, store, 0, scopeB); ok {
		t.Fatal("scope B gained a key row, want none")
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
	detail, err := store.RecordDetail(ctx, testCaptureID("k-a1"))
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
	captureID := testCaptureID("key-update")
	if err := store.AppendCapture(ctx, captureAt(captureID, scope, key1, "gpt-5", "gpt-5-mini", at), CaptureBodies{RequestBody: []byte("body")}); err != nil {
		t.Fatalf("AppendCapture: %v", err)
	}

	snapshot := func() string {
		t.Helper()
		var row string
		if err := store.shards[0].db.QueryRowContext(ctx,
			"SELECT client_key_id || '|' || model || '|' || requested_model || '|' || event_count || '|' || started_at_us || '|' || completed_at_us FROM captures WHERE capture_id = ?",
			mustCaptureIDBytes(t, captureID)).Scan(&row); err != nil {
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
	detail, err := store.RecordDetail(ctx, captureID)
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

// mustCaptureIDBytes is the raw form of a test capture ID.
func mustCaptureIDBytes(t *testing.T, id string) []byte {
	t.Helper()
	raw, err := ParseCaptureID(id)
	if err != nil {
		t.Fatalf("ParseCaptureID(%s): %v", id, err)
	}
	return raw
}

// TestSQLiteCaptureIDIsUniqueAndRandom pins the identity contract of the plugin:
// two generated IDs differ, and each is 32 lowercase hex characters describing 16
// bytes.
func TestSQLiteCaptureIDIsUniqueAndRandom(t *testing.T) {
	const draws = 4096
	seen := make(map[string]bool, draws)
	for i := 0; i < draws; i++ {
		id, err := NewCaptureID()
		if err != nil {
			t.Fatalf("NewCaptureID: %v", err)
		}
		if len(id) != CaptureIDHexLen {
			t.Fatalf("NewCaptureID returned %d characters, want %d", len(id), CaptureIDHexLen)
		}
		if strings.ToLower(id) != id {
			t.Fatalf("NewCaptureID returned %q, want lowercase hex", id)
		}
		if seen[id] {
			t.Fatalf("NewCaptureID repeated %s", id)
		}
		seen[id] = true
		raw, err := ParseCaptureID(id)
		if err != nil {
			t.Fatalf("ParseCaptureID(%s): %v", id, err)
		}
		if len(raw) != captureIDBytesLen {
			t.Fatalf("ParseCaptureID(%s) returned %d bytes, want %d", id, len(raw), captureIDBytesLen)
		}
	}
	for _, bad := range []string{"", "   ", "0", strings.Repeat("a", CaptureIDHexLen-1), strings.Repeat("a", CaptureIDHexLen+1), strings.Repeat("z", CaptureIDHexLen)} {
		if _, err := ParseCaptureID(bad); err == nil {
			t.Errorf("ParseCaptureID(%q) succeeded, want error", bad)
		}
	}
	// Uppercase hex names the same capture.
	upper := strings.ToUpper(seen0(t, seen))
	raw, err := ParseCaptureID(upper)
	if err != nil {
		t.Fatalf("ParseCaptureID(%s): %v", upper, err)
	}
	if got := formatCaptureID(raw); got != strings.ToLower(upper) {
		t.Fatalf("formatCaptureID = %q, want %q", got, strings.ToLower(upper))
	}
}

// seen0 returns any key of the set; it exists to avoid iterating a map in the test
// above without a defined order.
func seen0(t *testing.T, set map[string]bool) string {
	t.Helper()
	for id := range set {
		return id
	}
	t.Fatal("empty set")
	return ""
}

// TestShardIndexIsStableAndSpreads pins the fixed mapping recorded in the manifest.
func TestShardIndexIsStableAndSpreads(t *testing.T) {
	const shards = 4
	counts := make([]int, shards)
	for i := 0; i < 64; i++ {
		raw := mustCaptureIDBytes(t, testCaptureID(fmt.Sprintf("spread-%02d", i)))
		index := shardIndex(raw, shards)
		if index < 0 || index >= shards {
			t.Fatalf("shardIndex = %d, want 0..%d", index, shards-1)
		}
		// The mapping must be a pure function of the ID.
		if again := shardIndex(raw, shards); again != index {
			t.Fatalf("shardIndex is not stable: %d then %d", index, again)
		}
		counts[index]++
	}
	for index, n := range counts {
		if n == 0 {
			t.Fatalf("shard %d never received a record of 64: %v", index, counts)
		}
	}
	// A single shard always maps to 0, whatever the ID.
	if got := shardIndex(mustCaptureIDBytes(t, testCaptureID("single")), 1); got != 0 {
		t.Fatalf("shardIndex(id, 1) = %d, want 0", got)
	}
}

// TestSQLiteSearchWindowIsCappedForShardedStores pins the merge window limit. A
// sharded store has no global index, so it reads every shard from the top and
// materialises up to shards*(offset+limit) rows for one page; a window deeper than
// MaxSearchWindow is refused instead of being served at an unbounded cost. A store
// with a single database pushes the offset into the query, so it stays unlimited.
func TestSQLiteSearchWindowIsCappedForShardedStores(t *testing.T) {
	ctx := context.Background()

	sharded, _ := openTempSQLiteShards(t, 4)
	t.Cleanup(func() { _ = sharded.Close() })

	// The last allowed page of the maximum page size ends exactly at the window.
	edge := MaxSearchWindow / MaxPageSize
	if _, err := sharded.SearchRecords(ctx, RecordFilter{Page: edge, PageSize: MaxPageSize}); err != nil {
		t.Fatalf("SearchRecords at the window edge (page %d): %v", edge, err)
	}
	_, err := sharded.SearchRecords(ctx, RecordFilter{Page: edge + 1, PageSize: MaxPageSize})
	if !errors.Is(err, ErrSearchWindowTooDeep) {
		t.Fatalf("SearchRecords past the window error = %v, want ErrSearchWindowTooDeep", err)
	}
	if !strings.Contains(err.Error(), fmt.Sprint(MaxSearchWindow)) {
		t.Fatalf("window error %q must name the limit %d", err, MaxSearchWindow)
	}
	// The same page is fine once the filters narrow it: the window counts rows, not
	// pages, so a smaller page size reaches further.
	if _, err := sharded.SearchRecords(ctx, RecordFilter{Page: edge * 2, PageSize: MaxPageSize / 2}); err != nil {
		t.Fatalf("SearchRecords with a narrower page: %v", err)
	}

	single, _ := openTempSQLite(t)
	t.Cleanup(func() { _ = single.Close() })
	page, err := single.SearchRecords(ctx, RecordFilter{Page: 5000, PageSize: MaxPageSize})
	if err != nil {
		t.Fatalf("single-database deep page: %v", err)
	}
	if len(page.Items) != 0 || page.Total != 0 {
		t.Fatalf("single-database deep page = %d items / total %d, want 0 / 0", len(page.Items), page.Total)
	}
}

// TestSQLiteKeyReplicationAttemptsEveryShard breaks two shards of a four-shard
// store and requires AppendCapture to keep going: the capture itself must still be
// stored, the remaining healthy shard must still receive the key even though the
// earlier shards failed, and the failures must come back as ErrKeyIncomplete so the
// key stays eligible for a retry instead of the capture being counted as lost.
func TestSQLiteKeyReplicationAttemptsEveryShard(t *testing.T) {
	ctx := context.Background()
	store, _ := openTempSQLiteShards(t, 4)
	t.Cleanup(func() { _ = store.Close() })

	// Route the capture to shard 3 so the fan-out has to cross the broken shards.
	var captureID string
	for i := 0; i < 256 && captureID == ""; i++ {
		candidate := testCaptureID(fmt.Sprintf("fanout-%d", i))
		if shardIndex(mustCaptureIDBytes(t, candidate), 4) == 3 {
			captureID = candidate
		}
	}
	if captureID == "" {
		t.Fatal("no test capture ID mapped to shard 3")
	}
	for _, index := range []int{0, 1} {
		if err := store.shards[index].db.Close(); err != nil {
			t.Fatalf("close shard %d: %v", index, err)
		}
	}

	startedAt := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	capture := captureAt(captureID, "scope-fanout", "sk-fanout-secret", "gpt-fanout", "", startedAt)
	err := store.AppendCapture(ctx, capture, CaptureBodies{RequestBody: []byte("fanout-body")})
	if !errors.Is(err, ErrKeyIncomplete) {
		t.Fatalf("AppendCapture error = %v, want ErrKeyIncomplete", err)
	}
	// Both broken shards are reported: the fan-out did not stop at the first one.
	for _, index := range []int{0, 1} {
		if !strings.Contains(err.Error(), fmt.Sprintf("shard %d", index)) {
			t.Errorf("error %q does not report shard %d", err, index)
		}
	}
	// The capture is stored: it is readable by ID alone, body included.
	detail, err := store.RecordDetail(ctx, captureID)
	if err != nil {
		t.Fatalf("RecordDetail(%s): %v", captureID, err)
	}
	if got := string(detail.Bodies.RequestBody); got != "fanout-body" {
		t.Fatalf("stored body = %q, want %q", got, "fanout-body")
	}
	// Shard 2 is only reached after both failures, so its key proves the loop kept
	// going; shard 3 got the key inside the append transaction.
	for _, index := range []int{2, 3} {
		got, ok := clientKeyInShard(t, store, index, "scope-fanout")
		if !ok || got != "sk-fanout-secret" {
			t.Fatalf("shard %d key = %q (present %v), want sk-fanout-secret", index, got, ok)
		}
	}
	// The write is idempotent: once the shards are healthy again the same key is
	// accepted, which is what a retry does.
	if err := store.UpsertClientKey(ctx, "scope-fanout", "sk-fanout-secret"); err == nil {
		t.Fatal("UpsertClientKey on broken shards must report the failure")
	}
}
