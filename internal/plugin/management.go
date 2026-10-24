package plugin

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/mars160/cpa-distill/internal/panel"
)

const managementAPIBase = "/v0/management/plugins/cpa-distill"
const panelResourcePath = "/v0/resource/plugins/cpa-distill/records"

func okResult(value any) []byte {
	b, err := json.Marshal(map[string]any{"ok": true, "result": value})
	if err != nil {
		return failure("management_error", "cannot encode management response")
	}
	return b
}

// Routes have no Menu: in the host, GET + Menu is a PUBLIC resource route.
// Only the static HTML shell is declared in resources. Never return records there.
func (p *Plugin) registerManagement(raw []byte) []byte {
	routes := []map[string]string{
		{"Method": "POST", "Path": managementAPIBase + "/records/search"},
		{"Method": "POST", "Path": managementAPIBase + "/records/detail"},
		{"Method": "GET", "Path": managementAPIBase + "/records/options"},
		{"Method": "GET", "Path": managementAPIBase + "/status"},
	}
	return okResult(map[string]any{"routes": routes, "resources": []map[string]string{{"Path": panelResourcePath, "Menu": "调用记录", "Description": "按时间、客户端 Key、模型筛选成功调用"}}})
}

func (p *Plugin) handleManagement(raw []byte) []byte {
	var req struct {
		Method string
		Path   string
		Body   []byte
	}
	if json.Unmarshal(raw, &req) != nil {
		return failure("management_error", "invalid management request")
	}
	var response panel.Response
	if req.Path == panelResourcePath && req.Method == http.MethodGet {
		response = panel.StaticPage(managementAPIBase)
	} else if strings.HasPrefix(req.Path, managementAPIBase+"/") {
		// Host ServeManagementHTTP is reachable only after management middleware.
		// Serialize against reconfigure/close, not against traffic hooks.
		p.lifecycle.Lock()
		defer p.lifecycle.Unlock()
		p.mu.Lock()
		stats := panel.Stats{PendingRequests: len(p.pending), BufferBytes: p.bytes + p.keyBytes, Dropped: p.dropped.Load(), WriteFailures: p.writeFailures.Load()}
		var reader panel.Reader
		timeout := 5 * time.Second
		if b := p.current; b != nil {
			reader, _ = b.sink.(panel.Reader)
			stats.QueueLength = len(b.queue)
			timeout = b.cfg.Timeout()
		}
		p.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		response = panel.HandleAPI(ctx, req.Method, strings.TrimPrefix(req.Path, managementAPIBase), req.Body, reader, stats)
	} else {
		response = panel.Response{StatusCode: 404, Headers: http.Header{"Content-Type": {"application/json"}, "Cache-Control": {"no-store"}}, Body: []byte(`{"error":"not found"}`)}
	}
	// ManagementResponse has untagged Go field names; Body is []byte/base64 on ABI.
	return okResult(struct {
		StatusCode int
		Headers    http.Header
		Body       []byte
	}{response.StatusCode, response.Headers, response.Body})
}
