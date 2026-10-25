// Package panel implements the read-only records browser served by the host's
// authenticated plugin management API. It never opens a separate network port.
package panel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/mars160/cpa-distill/internal/storage"
)

const MaxRequestBody = 16 << 10

type Reader interface {
	SearchRecords(context.Context, storage.RecordFilter) (storage.RecordPage, error)
	RecordOptions(context.Context) (storage.RecordOptions, error)
	RecordDetail(context.Context, string) (storage.RecordDetail, error)
}

type Stats struct {
	PendingRequests int    `json:"pending_requests"`
	BufferBytes     int    `json:"buffer_bytes"`
	QueueLength     int    `json:"queue_length"`
	Writers         int    `json:"writers"`
	Shards          int    `json:"shards"`
	Dropped         uint64 `json:"dropped"`
	WriteFailures   uint64 `json:"write_failures"`
}

type Response struct {
	StatusCode int
	Headers    http.Header
	Body       []byte
}

func jsonResponse(status int, value any) Response {
	b, err := json.Marshal(value)
	if err != nil {
		return apiError(http.StatusInternalServerError, "无法编码查询结果")
	}
	return Response{StatusCode: status, Headers: http.Header{"Content-Type": {"application/json; charset=utf-8"}, "Cache-Control": {"no-store"}, "X-Content-Type-Options": {"nosniff"}}, Body: b}
}

func apiError(status int, message string) Response {
	return jsonResponse(status, map[string]string{"error": message})
}

func decode(body []byte, dest any) error {
	if len(body) > MaxRequestBody {
		return errors.New("request too large")
	}
	d := json.NewDecoder(strings.NewReader(string(body)))
	d.DisallowUnknownFields()
	if err := d.Decode(dest); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return errors.New("expected one JSON value")
	}
	return nil
}

// HandleAPI must only be called from routes protected by host management auth.
// No caller-supplied API key or SQL error is ever returned in an error message.
func HandleAPI(ctx context.Context, method, path string, body []byte, reader Reader, stats Stats) Response {
	switch path {
	case "/status":
		if method != http.MethodGet {
			return apiError(405, "请求方法不支持")
		}
		return jsonResponse(200, stats)
	case "/records/options":
		if method != http.MethodGet {
			return apiError(405, "请求方法不支持")
		}
		if reader == nil {
			return apiError(503, "记录存储尚未就绪")
		}
		options, err := reader.RecordOptions(ctx)
		if err != nil {
			return apiError(503, "无法读取筛选选项，请稍后重试")
		}
		return jsonResponse(200, options)
	case "/records/search":
		if method != http.MethodPost {
			return apiError(405, "请使用 POST 查询记录")
		}
		var req struct {
			From     string `json:"from"`
			To       string `json:"to"`
			KeyID    string `json:"key_id"`
			Model    string `json:"model"`
			Page     int    `json:"page"`
			PageSize int    `json:"page_size"`
		}
		if decode(body, &req) != nil {
			return apiError(400, "查询参数必须为合法 JSON")
		}
		filter := storage.RecordFilter{KeyID: req.KeyID, Model: req.Model, Page: req.Page, PageSize: req.PageSize}
		if filter.Page == 0 {
			filter.Page = 1
		}
		if filter.PageSize == 0 {
			filter.PageSize = 20
		}
		if filter.Page < 1 || filter.Page > 1000000 || filter.PageSize < 1 || filter.PageSize > 100 || len(req.KeyID) > 128 || len(req.Model) > 512 {
			return apiError(400, "分页或筛选参数不合法")
		}
		var err error
		if req.From != "" {
			filter.From, err = time.Parse(time.RFC3339Nano, req.From)
			if err != nil {
				return apiError(400, "开始时间必须为带时区的 RFC3339 时间")
			}
		}
		if req.To != "" {
			filter.To, err = time.Parse(time.RFC3339Nano, req.To)
			if err != nil {
				return apiError(400, "结束时间必须为带时区的 RFC3339 时间")
			}
		}
		if !filter.From.IsZero() && !filter.To.IsZero() && !filter.From.Before(filter.To) {
			return apiError(400, "结束时间必须晚于开始时间")
		}
		if reader == nil {
			return apiError(503, "记录存储尚未就绪")
		}
		result, err := reader.SearchRecords(ctx, filter)
		switch {
		case errors.Is(err, storage.ErrSearchWindowTooDeep):
			// 分片合并无法下推 offset，深页会物化 shards*(offset+limit) 条记录，
			// 因此超过窗口直接按客户端错误拒绝，而不是把内存交给调用方。
			return apiError(400, fmt.Sprintf("跨分片查询最多浏览前 %d 条记录（offset+limit ≤ %d），请缩窄筛选条件或按 capture_id 查看单条记录",
				storage.MaxSearchWindow, storage.MaxSearchWindow))
		case err != nil:
			return apiError(503, "记录查询失败，请稍后重试")
		}
		return jsonResponse(200, result)
	case "/records/detail":
		if method != http.MethodPost {
			return apiError(405, "请使用 POST 查询详情")
		}
		var req struct {
			CaptureID string `json:"capture_id"`
		}
		if decode(body, &req) != nil {
			return apiError(400, "缺少或无效的 capture_id")
		}
		// The detail view is addressed by capture ID alone. The shape is checked here
		// so a malformed ID is a client error instead of a store error.
		captureID := strings.TrimSpace(req.CaptureID)
		if _, err := storage.ParseCaptureID(captureID); err != nil {
			return apiError(400, "缺少或无效的 capture_id")
		}
		if reader == nil {
			return apiError(503, "记录存储尚未就绪")
		}
		result, err := reader.RecordDetail(ctx, captureID)
		if errors.Is(err, storage.ErrNotFound) {
			return apiError(404, "记录不存在")
		}
		if err != nil {
			return apiError(503, "无法读取记录详情，请稍后重试")
		}
		return jsonResponse(200, result)
	default:
		return apiError(404, "接口不存在")
	}
}
