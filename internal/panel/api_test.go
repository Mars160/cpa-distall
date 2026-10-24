package panel

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/mars160/cpa-distill/internal/storage"
)

type fakeReader struct {
	filter storage.RecordFilter
	err    error
}

func (f *fakeReader) SearchRecords(_ context.Context, filter storage.RecordFilter) (storage.RecordPage, error) {
	f.filter = filter
	return storage.RecordPage{Items: []storage.Capture{}, Page: filter.Page, PageSize: filter.PageSize}, f.err
}
func (f *fakeReader) RecordOptions(context.Context) (storage.RecordOptions, error) {
	return storage.RecordOptions{Models: []string{"model-a"}}, f.err
}
func (f *fakeReader) RecordDetail(context.Context, string, string) (storage.RecordDetail, error) {
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
	for _, endpoint := range []struct{ method, path, body string }{{"GET", "/records/options", ""}, {"POST", "/records/search", "{}"}, {"POST", "/records/detail", `{"session_id":"s","request_id":"r"}`}} {
		got := HandleAPI(context.Background(), endpoint.method, endpoint.path, []byte(endpoint.body), reader, Stats{})
		if got.StatusCode != 503 || strings.Contains(string(got.Body), "secret") {
			t.Fatalf("backend error leaked: %s", got.Body)
		}
		if got.Headers.Get("Cache-Control") != "no-store" {
			t.Fatal("sensitive response must not be cached")
		}
	}
	reader.err = storage.ErrNotFound
	if got := HandleAPI(context.Background(), "POST", "/records/detail", []byte(`{"session_id":"s","request_id":"r"}`), reader, Stats{}); got.StatusCode != 404 {
		t.Fatal("missing record not mapped to 404")
	}
	if got := HandleAPI(context.Background(), "POST", "/records/detail", []byte(`{"session_id":"s"}`), reader, Stats{}); got.StatusCode != 400 {
		t.Fatal("missing identifiers accepted")
	}
	if got := HandleAPI(context.Background(), "POST", "/records/search", []byte(strings.Repeat("x", MaxRequestBody+1)), reader, Stats{}); got.StatusCode != 400 {
		t.Fatal("oversized request accepted")
	}
}

func TestStatsJSON(t *testing.T) {
	expected := Stats{PendingRequests: 2, BufferBytes: 1024, QueueLength: 1, Dropped: 3, WriteFailures: 4}
	out := HandleAPI(context.Background(), "GET", "/status", nil, nil, expected)
	var actual Stats
	if err := json.Unmarshal(out.Body, &actual); err != nil || actual != expected {
		t.Fatalf("unexpected stats: %s", out.Body)
	}
	if out.StatusCode != 200 {
		t.Fatal(out.StatusCode)
	}
}
