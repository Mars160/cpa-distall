// Package plugin implements the CLIProxyAPI C-ABI JSON contract.
// Wire contract: CLIProxyAPI v8.0.29, ABI 1, schema 6.
package plugin

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mars160/cpa-distill/internal/config"
	"github.com/mars160/cpa-distill/internal/storage"
)

// Repository is plugin registration metadata, override with go build -ldflags '-X .../internal/plugin.Repository=...'.
// The default is the module path, not a claim that a public repository has been published.
var Repository = "https://github.com/mars160/cpa-distill"

// Sink is the persistence backend. The plugin stores one successful call as a
// summary row plus its raw request/response/websocket bytes; it never writes
// individual hook events and never generates event identifiers. UpsertClientKey
// writes the plaintext of one caller scope into the store's own key table.
type Sink interface {
	AppendCapture(context.Context, storage.Capture, storage.CaptureBodies) error
	UpsertClientKey(context.Context, string, string) error
	Close() error
}
type OpenFunc func(context.Context, storage.Config) (Sink, error)

// captureOverhead is the conservative per-capture accounting for the summary
// header, map entry and slice bookkeeping that are not part of the body bytes.
const captureOverhead = 256

// maxStoredKeys bounds the "already upserted" set of one backend. Eviction only
// costs a redundant, idempotent upsert, never correctness.
const maxStoredKeys = 4096

// pending is one in-flight call observed from its request.intercept_before.
type pending struct {
	capture       storage.Capture
	requestBody   []byte
	requestSeen   bool
	responseBody  []byte // non-stream response.intercept_after candidate
	streamBody    []byte // concatenated ChunkIndex >= 0 stream bodies
	streamSeen    bool   // a stream chunk event was observed, even an empty one
	websocketBody []byte // newline separated upstream websocket payloads
	events        int64
	size          int
	touched       time.Time
	dropped       bool
}

func pendingSize(item *pending) int {
	return len(item.requestBody) + len(item.responseBody) + len(item.streamBody) + len(item.websocketBody) +
		len(item.capture.RequestID) + len(item.capture.ClientKey) + captureOverhead
}

type batch struct {
	key     *clientKey
	capture storage.Capture
	bodies  storage.CaptureBodies
	size    int
}
type backend struct {
	sink    Sink
	queue   chan batch
	done    chan struct{}
	cfg     config.Config
	session string
	ctx     context.Context
	cancel  context.CancelFunc
	// keyPending marks keys queued but not yet written; keyStored marks keys the
	// backend already upserted. Both are cleared on reconfiguration, and a failed
	// write only clears keyPending so the key is retried instead of being lost.
	keyPending map[string]bool
	keyStored  map[string]bool
}

type Plugin struct {
	lifecycle     sync.Mutex
	mu            sync.Mutex
	current       *backend
	pending       map[string]*pending
	keys          map[string]cachedKey
	keyBytes      int
	bytes         int // Includes pending, queued and currently writing bytes.
	lastSweep     time.Time
	open          OpenFunc
	dropped       atomic.Uint64
	writeFailures atomic.Uint64
	reportedDrops atomic.Uint64
}

func New() *Plugin {
	return NewWithOpener(func(ctx context.Context, c storage.Config) (Sink, error) { return storage.Open(ctx, c) })
}
func NewWithOpener(open OpenFunc) *Plugin {
	return &Plugin{open: open, pending: make(map[string]*pending), keys: make(map[string]cachedKey)}
}
func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func (p *Plugin) configure(raw []byte) error {
	var req struct {
		ConfigYAML []byte `json:"config_yaml"`
	}
	if err := json.Unmarshal(raw, &req); err != nil {
		return errors.New("invalid lifecycle request")
	}
	cfg, err := config.Parse(string(req.ConfigYAML))
	if err != nil {
		return err
	}
	p.lifecycle.Lock()
	defer p.lifecycle.Unlock()
	p.mu.Lock()
	same := p.current != nil && p.current.cfg == cfg
	p.mu.Unlock()
	if same {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout())
	defer cancel()
	sink, err := p.open(ctx, storage.Config{Driver: cfg.Driver, DSN: cfg.DSN})
	if err != nil {
		return errors.New("cannot open persistence database; check driver, dsn and permissions")
	}
	session, err := newID()
	if err != nil {
		_ = sink.Close()
		return errors.New("cannot create session ID")
	}
	workerCtx, workerCancel := context.WithCancel(context.Background())
	next := &backend{
		sink: sink, queue: make(chan batch, cfg.QueueSize), done: make(chan struct{}), cfg: cfg,
		session: session, ctx: workerCtx, cancel: workerCancel,
		keyPending: make(map[string]bool), keyStored: make(map[string]bool),
	}
	go p.run(next)
	p.mu.Lock()
	old := p.current
	p.clearPending()
	p.clearKeys()
	p.current = next
	if old != nil {
		close(old.queue)
	}
	p.mu.Unlock()
	drain(old)
	return nil
}

// drain applies one budget to the entire queue, not one timeout per remaining item.
// Wait for the writer to exit before allowing native library unload.
func drain(b *backend) {
	if b == nil {
		return
	}
	timer := time.AfterFunc(b.cfg.DrainBudget(), b.cancel)
	<-b.done
	timer.Stop()
	b.cancel()
}

func (p *Plugin) run(b *backend) {
	defer close(b.done)
	defer b.cancel()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	defer p.reportDrops()
	for {
		var item batch
		select {
		case next, ok := <-b.queue:
			if !ok {
				if err := b.sink.Close(); err != nil {
					log.Print("cpa-distill: capture database close failed")
				}
				return
			}
			item = next
		case <-ticker.C:
			p.reportDrops()
			continue
		}
		if b.ctx.Err() != nil {
			p.noteDrop()
		} else {
			ctx, cancel := context.WithTimeout(b.ctx, b.cfg.Timeout())
			var err error
			if item.key != nil {
				err = b.sink.UpsertClientKey(ctx, item.key.id, item.key.value)
				p.mu.Lock()
				if p.current == b {
					delete(b.keyPending, item.key.id)
					if err == nil && len(b.keyStored) < maxStoredKeys {
						b.keyStored[item.key.id] = true
					}
				}
				p.mu.Unlock()
			} else {
				p.mu.Lock()
				if p.current == b && item.capture.ClientKey == "" {
					key := p.cachedClientKey(item.capture.ClientKeyID, time.Now())
					if len(key) <= b.cfg.MaxBufferBytes-p.bytes-p.keyBytes {
						item.capture.ClientKey = key
						item.size += len(key)
						p.bytes += len(key)
					}
				}
				p.mu.Unlock()
				err = b.sink.AppendCapture(ctx, item.capture, item.bodies)
			}
			cancel()
			if err != nil {
				n := p.writeFailures.Add(1)
				// SQL errors can include DSNs or captured content; never log them.
				if n == 1 || n%100 == 0 {
					log.Printf("cpa-distill: capture database write failed (total=%d); proxy traffic is unaffected", n)
				}
			}
		}
		p.mu.Lock()
		p.bytes -= item.size
		p.mu.Unlock()
	}
}

// clearPending is called with mu held. No partial/in-flight captures cross database changes.
func (p *Plugin) clearPending() {
	for _, item := range p.pending {
		p.bytes -= item.size
	}
	if len(p.pending) > 0 {
		p.noteDrop()
	}
	p.pending = make(map[string]*pending)
}

// Close drains already-accepted successful captures; unfinished requests are discarded.
func (p *Plugin) Close() error {
	p.lifecycle.Lock()
	defer p.lifecycle.Unlock()
	p.mu.Lock()
	old := p.current
	p.current = nil
	p.clearPending()
	p.clearKeys()
	if old != nil {
		close(old.queue)
	}
	p.mu.Unlock()
	drain(old)
	return nil
}

// Hook paths update counters only: even stderr I/O must not delay proxy traffic.
func (p *Plugin) noteDrop() { p.dropped.Add(1) }

func (p *Plugin) reportDrops() {
	n := p.dropped.Load()
	previous := p.reportedDrops.Swap(n)
	if n > previous {
		log.Printf("cpa-distill: captures skipped (total=%d); buffer limit, invalid event, expiry, or reconfiguration; proxy traffic is unaffected", n)
	}
}

// abortCapture drops one in-flight capture in full. It is used when a body field
// cannot be decoded: keeping the remaining bytes would present a truncated body
// as a complete capture.
//
// The capture is only dropped when it still belongs to the same backend and is
// still the same pending entry: a reconfigure or Close may have discarded it
// already, and a new backend must never lose an entry that merely shares its
// request ID.
func (p *Plugin) abortCapture(b *backend, item *pending, requestID string) {
	p.mu.Lock()
	if p.current == b && p.pending[requestID] == item {
		p.bytes -= item.size
		delete(p.pending, requestID)
	}
	p.mu.Unlock()
	p.noteDrop()
}

// bodyGrowth is the exact number of bytes this event adds to the capture if it is
// applied, computed from the base64 length without decoding the body. It mirrors
// the accumulation below, so the capacity check that runs before decoding never
// rejects a capture that would fit.
func bodyGrowth(method string, env *hookEnvelope, chunkIndex int, hasChunkIndex bool, item *pending) int {
	switch method {
	case "request.intercept_before":
		if item.requestSeen {
			return 0
		}
		return decodedBodyLen(env.Body) - len(item.requestBody)
	case "response.intercept_after":
		return decodedBodyLen(env.Body) - len(item.responseBody)
	case "response.intercept_stream_chunk":
		// ChunkIndex -1 is the stream initialisation and is not content.
		if hasChunkIndex && chunkIndex < 0 {
			return 0
		}
		return decodedBodyLen(env.Body)
	case "websocket.response_event":
		growth := decodedBodyLen(env.Payload)
		if len(item.websocketBody) > 0 {
			growth++ // separator
		}
		return growth
	default:
		return 0
	}
}

// record is best-effort and never returns a hook error. No database I/O occurs on
// the hook path and no hook envelope is re-encoded: only the raw request/response
// bytes and a few typed attributes are kept. Body bytes are decoded as late as
// possible, so a capture that already exceeded its budget costs nothing extra.
func (p *Plugin) record(method string, raw []byte) {
	if len(raw) == 0 {
		p.noteDrop()
		return
	}
	var env hookEnvelope
	if err := json.Unmarshal(raw, &env); err != nil || env.RequestID == "" {
		p.noteDrop()
		return
	}
	requestID := env.RequestID
	var outcome string
	if method == "request.complete" {
		outcome = decodeText(env.Outcome)
	}
	clientScope, clientKeyValue := clientIdentity(method, &env)
	now := time.Now().UTC()

	p.mu.Lock()
	b := p.current
	if b == nil {
		p.mu.Unlock()
		return
	}
	if now.Sub(p.lastSweep) >= time.Minute {
		for key, item := range p.pending {
			if now.Sub(item.touched) > b.cfg.TTL() {
				p.bytes -= item.size
				delete(p.pending, key)
				p.noteDrop()
			}
		}
		p.lastSweep = now
	}
	item := p.pending[requestID]
	// Capture only requests observed from their beginning. Missing/expired starts must not create partial records.
	if item == nil {
		if method != "request.intercept_before" {
			p.mu.Unlock()
			return
		}
		if len(p.pending) >= b.cfg.MaxPendingRequests {
			p.mu.Unlock()
			p.noteDrop()
			return
		}
		item = &pending{capture: storage.Capture{SessionID: b.session, RequestID: requestID, ClientKeyID: "unknown"}}
		item.size = pendingSize(item)
		p.pending[requestID] = item
		p.bytes += item.size
	}
	item.touched = now
	if method == "request.complete" && outcome != "succeeded" {
		p.bytes -= item.size
		delete(p.pending, requestID)
		p.mu.Unlock()
		return
	}
	if item.dropped {
		// Over budget already: stop before touching any body byte.
		p.mu.Unlock()
		return
	}
	// ChunkIndex is tiny and decides whether this chunk is content at all; a
	// malformed value makes the capture unusable and drops it whole.
	var chunkIndex int
	var hasChunkIndex bool
	if method == "response.intercept_stream_chunk" {
		var err error
		if chunkIndex, hasChunkIndex, err = decodeChunkIndex(env.ChunkIndex); err != nil {
			p.mu.Unlock()
			p.abortCapture(b, item, requestID)
			return
		}
	}
	// The decoded body size follows from the base64 length (padding included) and
	// is known without decoding, so an over-budget capture is rejected here and no
	// body byte is ever decoded for it. The estimate is exact for canonical base64.
	if growth := bodyGrowth(method, &env, chunkIndex, hasChunkIndex, item); growth > 0 {
		if item.size+growth > b.cfg.MaxRequestBytes || p.bytes+growth+p.keyBytes > b.cfg.MaxBufferBytes {
			p.bytes -= item.size
			delete(p.pending, requestID)
			p.mu.Unlock()
			p.noteDrop()
			return
		}
	}
	clientKey := clientKeyValue
	if clientKey == "" {
		clientKey = p.cachedClientKey(item.capture.ClientKeyID, now)
	}
	original := item
	p.mu.Unlock()

	// Decode body-bearing fields only now: a malformed value drops the whole
	// capture instead of storing the rest as if it were complete.
	var (
		body    []byte
		payload []byte
		err     error
	)
	switch method {
	case "request.intercept_before", "response.intercept_after", "response.intercept_stream_chunk":
		if body, err = decodeBodyBytes(env.Body); err != nil {
			p.abortCapture(b, original, requestID)
			return
		}
	case "websocket.response_event":
		if payload, err = decodeBodyBytes(env.Payload); err != nil {
			p.abortCapture(b, original, requestID)
			return
		}
	}

	p.mu.Lock()
	// The capture may have expired, been dropped or been superseded by a
	// reconfigure while the body was decoded. Applying the bytes to a different
	// backend - or queueing on a queue that no longer belongs to it - would corrupt
	// accounting or panic on a closed channel, so verify identity first.
	if p.current != b || p.pending[requestID] != original || original.dropped {
		p.mu.Unlock()
		return
	}
	item = original
	switch method {
	case "request.intercept_before":
		// The first before event is the client's original input; later upstream
		// request bodies (request.intercept_after) are never kept.
		if !item.requestSeen {
			item.requestBody = body
			item.requestSeen = true
		}
	case "response.intercept_after":
		// Non-stream response body. For a streamed call it is only a marker and is
		// discarded at completion in favour of the real chunk bytes.
		item.responseBody = body
	case "response.intercept_stream_chunk":
		item.streamSeen = true
		// ChunkIndex -1 is the stream initialisation and is not content.
		if !hasChunkIndex || chunkIndex >= 0 {
			item.streamBody = append(item.streamBody, body...)
		}
	case "websocket.response_event":
		// Upstream websocket payloads are kept separately, newline separated as a
		// boundary marker. This is not SSE framing and does not affect the response
		// bytes above.
		if len(item.websocketBody) > 0 {
			item.websocketBody = append(item.websocketBody, '\n')
		}
		item.websocketBody = append(item.websocketBody, payload...)
	}
	applySummary(&item.capture, &env, now)
	if clientScope != "" && clientScope != "unknown" {
		item.capture.ClientKeyID = clientScope
	}
	if clientKey != "" {
		item.capture.ClientKey = clientKey
		if clientScope != "" && clientScope != "unknown" {
			p.cacheKey(clientScope, clientKey, now)
		}
	}
	item.events++
	newSize := pendingSize(item)
	if newSize > b.cfg.MaxRequestBytes || p.bytes-item.size+newSize+p.keyBytes > b.cfg.MaxBufferBytes {
		p.bytes -= item.size
		delete(p.pending, requestID)
		p.mu.Unlock()
		p.noteDrop()
		return
	}
	p.bytes += newSize - item.size
	item.size = newSize
	if method == "request.complete" {
		delete(p.pending, requestID)
		if item.capture.CompletedAt.IsZero() {
			item.capture.CompletedAt = now
		}
		item.capture.EventCount = item.events
		bodies := storage.CaptureBodies{RequestBody: item.requestBody, WebSocketBody: item.websocketBody}
		// A streamed call keeps the chunk bytes even when they are empty; the
		// non-stream body is only used when no stream chunk was observed at all.
		if item.streamSeen {
			bodies.ResponseBody = item.streamBody
		} else {
			bodies.ResponseBody = item.responseBody
		}
		select {
		case b.queue <- batch{capture: item.capture, bodies: bodies, size: item.size}:
		default:
			p.bytes -= item.size
			p.noteDrop()
		}
	}
	p.mu.Unlock()
}

func (p *Plugin) Handle(method string, raw []byte) []byte {
	switch method {
	case "plugin.register", "plugin.reconfigure":
		if err := p.configure(raw); err != nil {
			return failure("plugin_error", err.Error())
		}
		out, _ := json.Marshal(map[string]any{"ok": true, "result": registration()})
		return out
	case "usage.handle":
		p.observeUsage(raw)
	case "management.register":
		return p.registerManagement(raw)
	case "management.handle":
		return p.handleManagement(raw)
	case "plugin.shutdown":
		_ = p.Close()
	case "plugin.quiesce":
		// No new writes originate outside incoming hooks. Final draining occurs on shutdown.
	case "request.intercept_before", "request.intercept_after", "response.intercept_after", "response.intercept_stream_chunk", "request.complete", "websocket.response_event":
		p.record(method, raw)
	default:
		return failure("unknown_method", "unsupported method")
	}
	// Empty modifications mean exact pass-through, including stream headers/chunks.
	return []byte(`{"ok":true,"result":{}}`)
}

func failure(code, message string) []byte {
	b, _ := json.Marshal(map[string]any{"ok": false, "error": map[string]string{"code": code, "message": message}})
	return b
}

func registration() any {
	return map[string]any{
		"schema_version": 6,
		"metadata": map[string]any{
			"Name": "cpa-distill", "Version": "0.2.1", "Author": "cpa-distill contributors", "GitHubRepository": Repository,
			"ConfigFields": []map[string]any{
				{"Name": "driver", "Type": "enum", "EnumValues": []string{"sqlite", "postgres"}, "Default": "sqlite", "Description": "Persistence backend"},
				{"Name": "dsn", "Type": "string", "Description": "SQLite file path or PostgreSQL DSN"},
				{"Name": "write_timeout", "Type": "string", "Default": "5s", "Description": "Background batch write timeout (max 1m)"},
				{"Name": "drain_timeout", "Type": "string", "Default": "5s", "Description": "Total queue drain budget on unload or reconfigure (max 1m)"},
				{"Name": "max_request_bytes", "Type": "integer", "Default": 64 << 20, "Description": "Per-request capture memory budget"},
				{"Name": "max_buffer_bytes", "Type": "integer", "Default": 256 << 20, "Description": "Total pending and queued capture memory budget"},
				{"Name": "max_pending_requests", "Type": "integer", "Default": 1024, "Description": "Maximum pending captures"},
				{"Name": "queue_size", "Type": "integer", "Default": 32, "Description": "Maximum queued successful captures"},
				{"Name": "request_ttl", "Type": "string", "Default": "30m", "Description": "Expire abandoned captures after inactivity"},
			},
		},
		"capabilities": map[string]bool{
			"request_interceptor": true, "request_lifecycle_plugin": true,
			"response_interceptor": true, "response_stream_interceptor": true,
			"websocket_response_observer": true, "usage_plugin": true, "management_api": true,
		},
	}
}
