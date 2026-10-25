// Package plugin implements the CLIProxyAPI C-ABI JSON contract.
// Wire contract: CLIProxyAPI v8.0.29, ABI 1, schema 6.
package plugin

import (
	"context"
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

// maxStreamChunkRefs bounds how many stream fragments one capture retains by
// reference. Above it the whole capture is skipped, never truncated. Every
// reference is also charged to the byte budget (capacity plus header), so the
// count is a safety valve on top of the memory bound rather than the primary
// limit; a normal stream stays far below it.
const maxStreamChunkRefs = 1 << 16

// streamChunkRefOverhead is the slice header one retained fragment reference
// costs on top of the buffer it points at.
const streamChunkRefOverhead = 24

// pending is one in-flight call observed from its request.intercept_before.
type pending struct {
	capture       storage.Capture
	requestBody   []byte
	requestSeen   bool
	responseBody  []byte   // non-stream response.intercept_after candidate
	streamChunks  [][]byte // references to decoded ChunkIndex >= 0 stream bodies
	chunkBytes    int      // running sum of cap(chunk), so a fragment is charged in O(1)
	streamSeen    bool     // a stream chunk event was observed, even an empty one
	websocketBody []byte   // newline separated upstream websocket payloads
	events        int64
	size          int
	touched       time.Time
	dropped       bool
}

// streamChunksSize is the memory a fragment list retains: one slice header per
// capacity slot plus the capacity of every referenced buffer. Capacity, not
// length, is what the process actually holds: a per-chunk decode buffer can be
// larger than the body it carries. The hook path never calls this: it accumulates
// pending.chunkBytes per fragment, and only the writer scans the list once.
func streamChunksSize(chunks [][]byte) int {
	total := cap(chunks) * streamChunkRefOverhead
	for _, c := range chunks {
		total += cap(c)
	}
	return total
}

func pendingSize(item *pending) int {
	return len(item.requestBody) + len(item.responseBody) + item.chunkBytes + cap(item.streamChunks)*streamChunkRefOverhead + len(item.websocketBody) +
		len(item.capture.CaptureID) + len(item.capture.RequestID) + len(item.capture.ClientKey) + captureOverhead
}

type batch struct {
	key          *clientKey
	capture      storage.Capture
	bodies       storage.CaptureBodies
	streamChunks [][]byte // fragment references the writer concatenates exactly once
	size         int
}
type backend struct {
	sink  Sink
	queue chan batch
	// wg tracks the writer goroutines of this backend: one per configured writer, all
	// consuming the single queue above. Only drain closes the queue, and every writer
	// then finishes the items it can still take before wg.Wait returns.
	wg     sync.WaitGroup
	cfg    config.Config
	ctx    context.Context
	cancel context.CancelFunc
	// keyPending marks keys queued but not yet written; keyStored marks keys the
	// backend already upserted. Both are cleared on reconfiguration, and a failed
	// write only clears keyPending, so the key stays eligible for a retry on a later
	// usage or capture instead of being lost.
	// With several writers they are shared state guarded by the plugin lock: a key
	// is only ever marked stored once every shard accepted it.
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
	keyFailures   atomic.Uint64
	reportedDrops atomic.Uint64
}

func New() *Plugin {
	return NewWithOpener(func(ctx context.Context, c storage.Config) (Sink, error) { return storage.Open(ctx, c) })
}
func NewWithOpener(open OpenFunc) *Plugin {
	return &Plugin{open: open, pending: make(map[string]*pending), keys: make(map[string]cachedKey)}
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
	sink, err := p.open(ctx, storage.Config{Driver: cfg.Driver, DSN: cfg.DSN, WriterCount: cfg.WriterCount})
	if err != nil {
		return errors.New("cannot open persistence database; check driver, dsn and permissions")
	}
	workerCtx, workerCancel := context.WithCancel(context.Background())
	next := &backend{
		sink: sink, queue: make(chan batch, cfg.QueueSize), cfg: cfg,
		ctx: workerCtx, cancel: workerCancel,
		keyPending: make(map[string]bool), keyStored: make(map[string]bool),
	}
	p.start(next)
	p.mu.Lock()
	old := p.current
	p.clearPending()
	p.clearKeys()
	p.current = next
	p.mu.Unlock()
	// The old backend is detached now, so no hook can enqueue into it any more and
	// drain may close its queue outside the lock.
	drain(old)
	return nil
}

// start launches the writer pool of one backend: writer_count goroutines share the
// single queue, so the queue capacity and the memory budget stay global and are
// never multiplied by the number of writers. Every writer can serve every shard,
// which is what makes the sharded SQLite layout and the PostgreSQL pool actually
// concurrent.
func (p *Plugin) start(b *backend) {
	workers := b.cfg.WriterCount
	if workers < 1 {
		workers = 1
	}
	for i := 0; i < workers; i++ {
		b.wg.Add(1)
		go p.run(b)
	}
}

// drain closes the queue of a detached backend and applies one budget to the
// entire remaining backlog, not one timeout per item. It then waits for the whole
// writer pool before releasing the sink, so the native library is never unloaded
// while a writer goroutine is still running. Callers must have detached the backend
// from the plugin first, so no hook can enqueue again while the queue is closed.
//
// The budget cancels the writers' context, but whether an in-flight database call
// actually stops is up to the driver: a stuck call is abandoned only if the driver
// honours ctx cancellation. This function therefore waits for the goroutines
// unconditionally. If a driver ignored cancellation, shutdown would block instead of
// returning while a database call is still running, which is deliberate: the host
// may unload the library as soon as Close returns, and a goroutine still inside the
// driver would then crash the process. The queue is already closed, so nothing new
// is accepted while it waits.
func drain(b *backend) {
	if b == nil {
		return
	}
	close(b.queue)
	timer := time.AfterFunc(b.cfg.DrainBudget(), b.cancel)
	b.wg.Wait()
	timer.Stop()
	b.cancel()
	if err := b.sink.Close(); err != nil {
		log.Print("cpa-distill: capture database close failed")
	}
}

func (p *Plugin) run(b *backend) {
	defer b.wg.Done()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	defer p.reportDrops()
	for {
		select {
		case item, ok := <-b.queue:
			if !ok {
				return
			}
			p.writeBatch(b, item)
		case <-ticker.C:
			p.reportDrops()
		}
	}
}

// writeBatch performs one queued write: a capture or a client-key upsert. The
// writer that took the batch is its only processor, so the key bookkeeping of that
// key is never updated by two writers at once.
func (p *Plugin) writeBatch(b *backend, item batch) {
	defer func() {
		p.mu.Lock()
		p.bytes -= item.size
		p.mu.Unlock()
	}()
	if b.ctx.Err() != nil {
		p.noteDrop()
		return
	}
	ctx, cancel := context.WithTimeout(b.ctx, b.cfg.Timeout())
	var err error
	if item.key != nil {
		err = p.writeClientKey(b, ctx, item.key)
	} else {
		err = p.writeCapture(b, ctx, &item)
	}
	cancel()
	if err == nil {
		return
	}
	if errors.Is(err, storage.ErrKeyIncomplete) {
		// The capture itself is stored; only the client key could not be replicated to
		// every shard. The plugin does not retry on its own, but the key stays eligible
		// for a retry on a later usage or capture, so this is reported separately from a
		// failed capture write and never counted as data loss.
		n := p.keyFailures.Add(1)
		if n == 1 || n%100 == 0 {
			log.Printf("cpa-distill: client key replication incomplete (total=%d); captures are stored and the key stays eligible for retry on a later usage or capture", n)
		}
		return
	}
	n := p.writeFailures.Add(1)
	// SQL errors can include DSNs or captured content; never log them.
	if n == 1 || n%100 == 0 {
		log.Printf("cpa-distill: capture database write failed (total=%d); proxy traffic is unaffected", n)
	}
}

// writeClientKey upserts one queued usage key. Only the writer goroutine that took
// the item runs this, and every access to the keyStored/keyPending maps happens
// under the plugin lock, so a key a capture already stored is skipped instead of
// upserted a second time; either way the pending mark and the byte budget are
// released. A failed write never marks the key, so the key stays eligible for a
// retry on a later usage or capture. There is no background retry loop: the plugin
// never re-enqueues the key on its own.
func (p *Plugin) writeClientKey(b *backend, ctx context.Context, key *clientKey) error {
	p.mu.Lock()
	already := p.current == b && b.keyStored[key.id]
	p.mu.Unlock()
	var err error
	if !already {
		err = b.sink.UpsertClientKey(ctx, key.id, key.value)
	}
	p.mu.Lock()
	if p.current == b {
		delete(b.keyPending, key.id)
		if err == nil && len(b.keyStored) < maxStoredKeys {
			b.keyStored[key.id] = true
		}
	}
	p.mu.Unlock()
	return err
}

// writeCapture persists one successful capture. Stream fragments arrive as
// references to their decoded bytes and are concatenated exactly once here, off
// the hook path.
//
// max_request_bytes bounds one retained capture, fragment references included; it
// does not include the merged copy, which exists only during this call. That copy
// is therefore bounded only by the global max_buffer_bytes budget: it is reserved
// before the copy and the references it replaces are released after it, so the
// transient peak is accounted for instead of silently exceeding the budget. When
// the copy cannot fit the remaining global budget the capture is skipped whole
// (never truncated) and never retried: a writer must not block waiting for room,
// so a tight budget drops a stream instead of deadlocking the queue.
func (p *Plugin) writeCapture(b *backend, ctx context.Context, item *batch) error {
	streamCost, mergedLen := 0, 0
	if len(item.streamChunks) > 0 {
		streamCost = streamChunksSize(item.streamChunks)
		for _, c := range item.streamChunks {
			mergedLen += len(c)
		}
	}
	p.mu.Lock()
	if p.current == b && item.capture.ClientKey == "" {
		key := p.cachedClientKey(item.capture.ClientKeyID, time.Now())
		if len(key) <= b.cfg.MaxBufferBytes-p.bytes-p.keyBytes {
			item.capture.ClientKey = key
			item.size += len(key)
			p.bytes += len(key)
		}
	}
	scope := item.capture.ClientKeyID
	// A key the backend already stored needs no second upsert: clearing it keeps
	// the capture row and the client_keys join intact without per-capture SQL.
	if item.capture.ClientKey != "" && b.keyStored[scope] {
		item.size -= len(item.capture.ClientKey)
		p.bytes -= len(item.capture.ClientKey)
		item.capture.ClientKey = ""
	}
	// The merge is a transient allocation on top of the fragments already charged
	// to p.bytes, so it must fit the remaining global budget. If it does not, the
	// whole capture is skipped here: run still releases item.size, and a drop is not
	// a write failure.
	if len(item.streamChunks) > 0 && mergedLen > b.cfg.MaxBufferBytes-p.bytes-p.keyBytes {
		item.streamChunks = nil
		p.mu.Unlock()
		p.noteDrop()
		return nil
	}
	p.bytes += mergedLen // reserve the merged buffer while the fragments still exist
	p.mu.Unlock()

	if len(item.streamChunks) > 0 {
		item.bodies.ResponseBody = mergeStreamChunks(item.streamChunks)
		// Release the fragment references, and the slice headers they occupy, as soon
		// as the single buffer holds their bytes.
		item.streamChunks = nil
		p.mu.Lock()
		p.bytes -= streamCost // the merge replaced the references with one buffer
		p.mu.Unlock()
		item.size += mergedLen - streamCost
	}

	err := b.sink.AppendCapture(ctx, item.capture, item.bodies)
	if err == nil && item.capture.ClientKey != "" && scope != "" && scope != "unknown" {
		p.mu.Lock()
		if p.current == b && len(b.keyStored) < maxStoredKeys {
			b.keyStored[scope] = true
		}
		p.mu.Unlock()
	}
	return err
}

// mergeStreamChunks concatenates fragment references into one exact-sized buffer
// and drops each reference as soon as its bytes are copied, so a merge never holds
// more than the fragments not yet copied on top of the result. It allocates only
// the data itself, never a fixed per-request block.
func mergeStreamChunks(chunks [][]byte) []byte {
	total := 0
	for _, c := range chunks {
		total += len(c)
	}
	if total == 0 {
		return nil
	}
	merged := make([]byte, 0, total)
	for i, c := range chunks {
		merged = append(merged, c...)
		chunks[i] = nil
	}
	return merged
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
// It returns only after every writer goroutine has exited and the sink is closed, so
// the host may unload the plugin library once Close returns. How long that takes is
// bounded by the drain budget only as far as the database driver honours context
// cancellation (see drain): the plugin never returns while a writer is still inside
// the driver.
func (p *Plugin) Close() error {
	p.lifecycle.Lock()
	defer p.lifecycle.Unlock()
	p.mu.Lock()
	old := p.current
	p.current = nil
	p.clearPending()
	p.clearKeys()
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

// bodyGrowth is the number of body bytes this event adds to the capture if it is
// applied, computed from the base64 length without decoding the body. It mirrors
// the accumulation below, so the capacity check that runs before decoding never
// rejects a capture that would fit: a stream fragment is charged its length here
// and its capacity plus slice header by the exact post-decode check.
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
	// A fresh 128-bit capture ID is drawn for a potential new capture before the
	// plugin lock is taken: crypto/rand is cheap, but it must never run while the
	// lock is held, because that lock is shared with every hook and with the writer
	// pool. An ID drawn for a call that already has a pending capture is discarded.
	// The host request ID is stored verbatim with the capture, for support only: it is
	// not an identity and never used to look a record up.
	var captureID string
	if method == "request.intercept_before" {
		id, err := storage.NewCaptureID()
		if err != nil {
			// Without an ID the capture cannot be stored or looked up later; dropping
			// it leaves the request itself untouched.
			p.noteDrop()
			return
		}
		captureID = id
	}

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
		item = &pending{capture: storage.Capture{CaptureID: captureID, RequestID: requestID, ClientKeyID: "unknown"}}
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
	// Typed attributes are decoded here, outside the lock: the hook path then only
	// assigns the parsed values once the capture is known to still be this backend's.
	sum := parseSummary(&env)

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
		// ChunkIndex -1 is the stream initialisation and is not content; an empty
		// chunk carries no content either, so neither is retained as a reference.
		if (!hasChunkIndex || chunkIndex >= 0) && len(body) > 0 {
			if len(item.streamChunks) >= maxStreamChunkRefs {
				// The fragment bound is explicit: skip the whole capture, never keep a
				// prefix of the stream.
				p.bytes -= item.size
				delete(p.pending, requestID)
				p.mu.Unlock()
				p.noteDrop()
				return
			}
			item.streamChunks = append(item.streamChunks, body)
			item.chunkBytes += cap(body)
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
	applySummary(&item.capture, sum, now)
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
		var chunks [][]byte
		// A streamed call keeps the chunk bytes even when they are empty; the
		// non-stream body is only used when no stream chunk was observed at all.
		if item.streamSeen {
			chunks = item.streamChunks
		} else {
			bodies.ResponseBody = item.responseBody
		}
		select {
		case b.queue <- batch{capture: item.capture, bodies: bodies, size: item.size, streamChunks: chunks}:
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
				{"Name": "dsn", "Type": "string", "Description": "SQLite data directory (manifest plus shard files) or PostgreSQL DSN"},
				{"Name": "writer_count", "Type": "integer", "Default": 1, "Description": "Concurrent writers: SQLite shard databases, or PostgreSQL pool size (1-64)"},
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
