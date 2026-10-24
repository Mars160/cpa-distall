package plugin

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mars160/cpa-distill/internal/storage"
)

type observedStore struct {
	*storage.Store
	appended chan error
	resolved chan error
}

func (s *observedStore) AppendCapture(ctx context.Context, c storage.Capture, b storage.CaptureBodies) error {
	err := s.Store.AppendCapture(ctx, c, b)
	s.appended <- err
	return err
}
func (s *observedStore) UpsertClientKey(ctx context.Context, id, key string) error {
	err := s.Store.UpsertClientKey(ctx, id, key)
	s.resolved <- err
	return err
}
func waitStorage(t *testing.T, ch <-chan error) {
	t.Helper()
	select {
	case err := <-ch:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("writer did not finish")
	}
}
func managementCall(t *testing.T, p *Plugin, method, path string, body any) (int, []byte) {
	t.Helper()
	var payload []byte
	if body != nil {
		payload = mustMarshal(body)
	}
	raw := p.Handle("management.handle", mustMarshal(map[string]any{"Method": method, "Path": path, "Body": payload}))
	var response struct {
		OK     bool `json:"ok"`
		Result struct {
			StatusCode int
			Body       []byte
			Headers    http.Header
		} `json:"result"`
	}
	if err := json.Unmarshal(raw, &response); err != nil || !response.OK {
		t.Fatalf("management response: %s / %v", raw, err)
	}
	if response.Result.Headers.Get("Cache-Control") != "no-store" {
		t.Fatal("sensitive management responses must not be cached")
	}
	return response.Result.StatusCode, response.Result.Body
}

func TestManagementRoutesArePrivateAndStaticShellPublic(t *testing.T) {
	p := New()
	var declaration struct {
		OK     bool `json:"ok"`
		Result struct {
			Routes    []struct{ Method, Path, Menu string } `json:"routes"`
			Resources []struct{ Path, Menu string }         `json:"resources"`
		} `json:"result"`
	}
	if err := json.Unmarshal(p.Handle("management.register", []byte(`{}`)), &declaration); err != nil {
		t.Fatal(err)
	}
	if !declaration.OK || len(declaration.Result.Routes) != 4 || len(declaration.Result.Resources) != 1 {
		t.Fatal("unexpected route declarations")
	}
	for _, r := range declaration.Result.Routes {
		if r.Menu != "" || !strings.HasPrefix(r.Path, managementAPIBase+"/") {
			t.Fatal("query route would become public")
		}
	}
	if declaration.Result.Resources[0].Path != panelResourcePath {
		t.Fatal("wrong resource entry")
	}
	status, html := managementCall(t, p, "GET", panelResourcePath, nil)
	if status != 200 || !strings.Contains(string(html), managementAPIBase) || strings.Contains(string(html), "__CPA_DISTILL_API_BASE__") {
		t.Fatal("static page was not embedded/substituted")
	}
	status, _ = managementCall(t, p, "GET", panelResourcePath+"/records/search", nil)
	if status != 404 {
		t.Fatal("public resource prefix must not dispatch data API")
	}
	status, _ = managementCall(t, p, "GET", managementAPIBase+"/records/options", nil)
	if status != 503 {
		t.Fatal("missing store not handled")
	}
}

func TestPanelCaptureAndLateUsageKeyResolution(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.db")
	store, err := storage.Open(context.Background(), storage.Config{Driver: "sqlite", DSN: path})
	if err != nil {
		t.Fatal(err)
	}
	sink := &observedStore{Store: store, appended: make(chan error, 8), resolved: make(chan error, 8)}
	p := NewWithOpener(func(context.Context, storage.Config) (Sink, error) { return sink, nil })
	register(t, p, configYAML(""))
	t.Cleanup(func() { _ = p.Close() })
	key := "sk-client-very-secret-query-auth"
	scope := keyID(key)
	metadata := map[string]any{"caller_scope": scope}
	started := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	// The client authenticated through a query parameter. Headers deliberately hold
	// another secret: only verified host caller scope may attribute the capture.
	send(t, p, "request.intercept_before", map[string]any{"RequestID": "r", "Model": "alias", "Metadata": metadata, "Headers": map[string][]string{"Authorization": {"Bearer sk-not-the-client"}}, "Body": []byte(`{"messages":[{"content":"hello"}]}`)})
	send(t, p, "request.intercept_after", map[string]any{"RequestID": "r", "Model": "model-upstream", "RequestedModel": "alias", "Metadata": metadata, "Headers": map[string][]string{"Authorization": {"Bearer sk-upstream-secret"}}, "Body": []byte("upstream request body")})
	send(t, p, "request.complete", map[string]any{"RequestID": "r", "Outcome": "succeeded", "Model": "model-upstream", "RequestedModel": "alias", "Metadata": metadata, "StartedAt": started, "CompletedAt": started.Add(time.Second)})
	waitStorage(t, sink.appended)
	status, body := managementCall(t, p, "POST", managementAPIBase+"/records/search", map[string]any{"page": 1, "page_size": 20, "key_id": scope, "model": "alias", "from": started.Format(time.RFC3339Nano), "to": started.Add(time.Second).Format(time.RFC3339Nano)})
	var page storage.RecordPage
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	if status != 200 || page.Total != 1 || len(page.Items) != 1 || page.Items[0].Model != "model-upstream" {
		t.Fatalf("query failed: %s", body)
	}
	// Usage RequestID differs intentionally; matching must use caller scope only.
	send(t, p, "usage.handle", map[string]any{"RequestID": "unrelated-usage-id", "APIKey": key, "Failed": false})
	waitStorage(t, sink.resolved)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var savedKey string
	if err := db.QueryRow("SELECT client_key FROM client_keys WHERE key_id = ?", scope).Scan(&savedKey); err != nil || savedKey != key {
		t.Fatalf("client key not resolved: %v", err)
	}
	status, options := managementCall(t, p, "GET", managementAPIBase+"/records/options", nil)
	if status != 200 || strings.Contains(string(options), key) || !strings.Contains(string(options), scope) {
		t.Fatalf("bad key options: %s", options)
	}
	status, detail := managementCall(t, p, "POST", managementAPIBase+"/records/detail", map[string]any{"session_id": page.Items[0].SessionID, "request_id": "r"})
	var decoded storage.RecordDetail
	if err := json.Unmarshal(detail, &decoded); err != nil {
		t.Fatal(err)
	}
	if status != 200 || string(decoded.Bodies.RequestBody) != `{"messages":[{"content":"hello"}]}` {
		t.Fatalf("bad detail: %s", detail)
	}
	// The upstream request body is never stored; only the client's original input is.
	if strings.Contains(string(decoded.Bodies.RequestBody), "upstream request body") {
		t.Fatal("upstream request body leaked into the stored request bytes")
	}
	for name, raw := range map[string][]byte{"request": decoded.Bodies.RequestBody, "response": decoded.Bodies.ResponseBody, "websocket": decoded.Bodies.WebSocketBody} {
		if strings.Contains(string(raw), "sk-") || strings.Contains(string(raw), "caller_scope") {
			t.Fatalf("%s body leaked credential metadata: %q", name, raw)
		}
	}
	if strings.Contains(string(detail), key) {
		t.Fatal("plaintext key leaked in detail")
	}
	// Resolving a key with only a failed call cannot create a capture or event row.
	failedKey := "sk-failed-no-record"
	send(t, p, "request.intercept_before", map[string]any{"RequestID": "failed", "Metadata": map[string]any{"caller_scope": keyID(failedKey)}})
	send(t, p, "request.complete", map[string]any{"RequestID": "failed", "Outcome": "failed"})
	send(t, p, "usage.handle", map[string]any{"APIKey": failedKey})
	waitStorage(t, sink.resolved)
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM captures WHERE request_id = ?", "failed").Scan(&count); err != nil || count != 0 {
		t.Fatal("failed call was recorded")
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.keys) != 0 || p.keyBytes != 0 || p.bytes != 0 {
		t.Fatal("key cache/buffers not released on shutdown")
	}
}

func TestClientIdentityRejectsUpstreamSecrets(t *testing.T) {
	key := "sk-client-valid"
	scope := keyID(key)
	for _, tc := range []struct{ method, scope, header, want string }{
		{"request.intercept_before", scope, "Bearer " + key, key},
		{"request.intercept_before", scope, "Bearer upstream-secret", ""},
		{"request.intercept_after", scope, "Bearer " + key, ""},
		{"request.intercept_before", "", "Bearer " + key, ""},
	} {
		env := hookEnvelope{
			Metadata: mustMarshal(map[string]any{"caller_scope": tc.scope}),
			Headers:  mustMarshal(map[string][]string{"Authorization": {tc.header}}),
		}
		_, raw := clientIdentity(tc.method, &env)
		if raw != tc.want {
			t.Fatalf("%s: inappropriate credential attribution", tc.method)
		}
	}
}
