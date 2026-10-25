package panel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/mars160/cpa-distill/internal/storage"
)

type fakeReader struct {
	filter    storage.RecordFilter
	detailID  string
	err       error
	searchErr error
}

func (f *fakeReader) SearchRecords(_ context.Context, filter storage.RecordFilter) (storage.RecordPage, error) {
	f.filter = filter
	err := f.searchErr
	if err == nil {
		err = f.err
	}
	return storage.RecordPage{Items: []storage.Capture{}, Page: filter.Page, PageSize: filter.PageSize}, err
}
func (f *fakeReader) RecordOptions(context.Context) (storage.RecordOptions, error) {
	return storage.RecordOptions{Models: []string{"model-a"}}, f.err
}
func (f *fakeReader) RecordDetail(_ context.Context, captureID string) (storage.RecordDetail, error) {
	f.detailID = captureID
	return storage.RecordDetail{}, f.err
}

func TestSearchValidationAndFilters(t *testing.T) {
	reader := &fakeReader{}
	body := []byte(`{"from":"2026-10-15T10:00:00+08:00","to":"2026-10-16T10:00:00+08:00","key_id":"abc","model":"model-a","page":2,"page_size":50}`)
	out := HandleAPI(context.Background(), "POST", "/records/search", body, reader, Stats{})
	if out.StatusCode != 200 || reader.filter.KeyID != "abc" || reader.filter.Model != "model-a" || reader.filter.Page != 2 || reader.filter.PageSize != 50 || reader.filter.From.UTC().Hour() != 2 {
		t.Fatalf("bad response/filter: %+v / %+v", out, reader.filter)
	}
	for _, body := range []string{`{`, `{"page":-1}`, `{"page_size":101}`, `{"from":"not-a-time"}`, `{"from":"2026-10-15T12:00:00Z","to":"2026-10-15T11:00:00Z"}`, `{} {}`, `{"client_key":"secret"}`, `{"model":"` + strings.Repeat("x", 513) + `"}`} {
		if got := HandleAPI(context.Background(), "POST", "/records/search", []byte(body), reader, Stats{}); got.StatusCode != 400 {
			t.Errorf("accepted invalid search: %s", body)
		}
	}
	if got := HandleAPI(context.Background(), "POST", "/records/search", []byte(`{}`), reader, Stats{}); got.StatusCode != 200 || reader.filter.Page != 1 || reader.filter.PageSize != 20 {
		t.Fatal("defaults not applied")
	}
}

// TestSearchWindowRefusalIsAClientError pins the API contract of the merge window: a
// sharded store refuses a page beyond its window, and the panel answers with a client
// error (400) naming the window and the alternatives - not with a storage outage (503)
// and not with the storage error text.
func TestSearchWindowRefusalIsAClientError(t *testing.T) {
	reader := &fakeReader{searchErr: fmt.Errorf("%w: offset+limit is 10100, internal detail", storage.ErrSearchWindowTooDeep)}
	out := HandleAPI(context.Background(), "POST", "/records/search", []byte(`{"page":101,"page_size":100}`), reader, Stats{})
	if out.StatusCode != 400 {
		t.Fatalf("status = %d, want 400 (body %s)", out.StatusCode, out.Body)
	}
	var payload struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(out.Body, &payload); err != nil {
		t.Fatalf("decode error payload: %v", err)
	}
	if !strings.Contains(payload.Error, fmt.Sprint(storage.MaxSearchWindow)) || !strings.Contains(payload.Error, "capture_id") {
		t.Fatalf("error message %q must name the window and the alternative", payload.Error)
	}
	if strings.Contains(payload.Error, "internal detail") {
		t.Fatalf("error message %q leaks the storage error", payload.Error)
	}
}

func TestReadOnlyMethodsErrorsAndNoStore(t *testing.T) {
	for _, path := range []string{"/records/search", "/records/detail"} {
		if got := HandleAPI(context.Background(), "GET", path, nil, nil, Stats{}); got.StatusCode != 405 {
			t.Fatalf("GET %s must not put sensitive search data in URL", path)
		}
	}
	if got := HandleAPI(context.Background(), "GET", "/records/options", nil, nil, Stats{}); got.StatusCode != 503 {
		t.Fatal("missing store not detected")
	}
	reader := &fakeReader{err: errors.New("postgres://user:secret@host body-secret")}
	for _, endpoint := range []struct{ method, path, body string }{{"GET", "/records/options", ""}, {"POST", "/records/search", "{}"}, {"POST", "/records/detail", `{"capture_id":"0123456789abcdef0123456789abcdef"}`}} {
		got := HandleAPI(context.Background(), endpoint.method, endpoint.path, []byte(endpoint.body), reader, Stats{})
		if got.StatusCode != 503 || strings.Contains(string(got.Body), "secret") {
			t.Fatalf("backend error leaked: %s", got.Body)
		}
		if got.Headers.Get("Cache-Control") != "no-store" {
			t.Fatal("sensitive response must not be cached")
		}
	}
	reader.err = storage.ErrNotFound
	const captureID = "0123456789abcdef0123456789abcdef"
	if got := HandleAPI(context.Background(), "POST", "/records/detail", []byte(`{"capture_id":"`+captureID+`"}`), reader, Stats{}); got.StatusCode != 404 {
		t.Fatal("missing record not mapped to 404")
	}
	// The detail view is addressed by capture ID alone: a legacy session/request
	// payload, an unknown field or a malformed ID is refused instead of guessed.
	for _, body := range []string{
		`{"session_id":"s","request_id":"r"}`,
		`{}`,
		`{"capture_id":""}`,
		`{"capture_id":"short"}`,
		`{"capture_id":"` + strings.Repeat("z", 32) + `"}`,
	} {
		if got := HandleAPI(context.Background(), "POST", "/records/detail", []byte(body), reader, Stats{}); got.StatusCode != 400 {
			t.Fatalf("accepted invalid detail request %s: %s", body, got.Body)
		}
	}
	reader.err = nil
	if got := HandleAPI(context.Background(), "POST", "/records/detail", []byte(`{"capture_id":"`+captureID+`"}`), reader, Stats{}); got.StatusCode != 200 {
		t.Fatalf("valid capture ID rejected: %s", got.Body)
	}
	if reader.detailID != captureID {
		t.Fatalf("detail lookup used %q, want %q", reader.detailID, captureID)
	}
	if got := HandleAPI(context.Background(), "POST", "/records/search", []byte(strings.Repeat("x", MaxRequestBody+1)), reader, Stats{}); got.StatusCode != 400 {
		t.Fatal("oversized request accepted")
	}
}

func TestStatsJSON(t *testing.T) {
	expected := Stats{PendingRequests: 2, BufferBytes: 1024, QueueLength: 1, Writers: 4, Shards: 4, Dropped: 3, WriteFailures: 4}
	out := HandleAPI(context.Background(), "GET", "/status", nil, nil, expected)
	var actual Stats
	if err := json.Unmarshal(out.Body, &actual); err != nil || actual != expected {
		t.Fatalf("unexpected stats: %s", out.Body)
	}
	if out.StatusCode != 200 {
		t.Fatal(out.StatusCode)
	}
	for _, field := range []string{`"writers":4`, `"shards":4`} {
		if !strings.Contains(string(out.Body), field) {
			t.Fatalf("status JSON is missing %s: %s", field, out.Body)
		}
	}
}
