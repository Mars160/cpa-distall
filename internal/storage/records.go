package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// UnknownKeyID is the KeyOption.ID reported for records whose client key is not
// identified (an empty client_key_id). It is not a sha256 digest, so it can never
// collide with a key ID supplied by the plugin.
const UnknownKeyID = "unknown"

// MaxPageSize is the largest PageSize SearchRecords accepts.
const MaxPageSize = 100

// maxMergePrefetch bounds the capacity reserved for the merged shard scan. The
// merge itself grows as needed; the hint only avoids a large up-front allocation
// when a deep page is requested.
const maxMergePrefetch = 1024

// MaxSearchWindow is the largest offset+limit SearchRecords accepts from a sharded
// store. A sharded store has no global index, so a page cannot be answered with a
// pushed-down offset: every shard must be scanned from its own top and the page is
// sliced out of the merge, which means one page materialises up to
// shards*(offset+limit) summaries. This constant bounds that window, so the worst
// case stays bounded (writer_count is at most MaxWriterCount) instead of growing
// with the offset until the process runs out of memory.
//
// A store that keeps one database (writer_count = 1, or PostgreSQL) is not
// affected: its page is answered by one OFFSET/LIMIT query, so deep pages cost no
// extra memory and stay available. The panel disables paging past the window and
// says so; deep browsing is meant to go through narrower filters or a capture ID.
const MaxSearchWindow = 10000

// Capture is the stored summary of one client call. It is the list row of the
// record browser and the header of the record detail view.
//
// CaptureID is the identity of the call: 32 lowercase hex characters describing
// the 128 bits the plugin drew from crypto/rand at the first hook. It is also the
// key that locates the record: the shard is derived from it and the detail view is
// fetched by it alone.
//
// RequestID is the host request ID exactly as the host reported it, kept verbatim
// for support and log correlation. It is deliberately not an identity: it carries
// no uniqueness constraint, is not indexed, and must never be used to look a record
// up (a host may reuse or omit it).
//
// ClientKey holds the client API key plaintext, which is authorised by the operator
// so a call can be attributed to a key; the field is tagged json:"-" and is
// therefore never part of any JSON representation. ClientKeyID is an opaque
// identifier supplied by the plugin: the caller scope of the upstream request. The
// literal "unknown" is accepted as an alias for "" (the plugin uses it when the
// host exposes no client key metadata) and is normalised on write, so the stored
// value is either a scope or empty.
//
// Only the scope is stored with the summary: the plaintext lives in the client_keys
// table (see Store.UpsertClientKey) and is joined in when the filter options are
// built, so no summary is ever rewritten when the key becomes known.
//
// EventCount is the number of hooks observed for the call. StartedAt and CompletedAt
// are stored as epoch microseconds; the record list is filtered and ordered by
// StartedAt.
type Capture struct {
	CaptureID      string    `json:"capture_id"`
	RequestID      string    `json:"request_id"`
	ClientKey      string    `json:"-"`
	ClientKeyID    string    `json:"client_key_id"`
	Model          string    `json:"model"`
	RequestedModel string    `json:"requested_model"`
	SourceFormat   string    `json:"source_format"`
	Stream         bool      `json:"stream"`
	StartedAt      time.Time `json:"started_at"`
	CompletedAt    time.Time `json:"completed_at"`
	EventCount     int64     `json:"event_count"`
}

// CaptureBodies is the raw body payload of one captured call. The bytes are stored
// verbatim and never re-encoded; they serialise as base64 (encoding/json's default
// for []byte). WebSocketBody holds the assembled stream/websocket frames and is
// empty for a non-streaming call.
type CaptureBodies struct {
	RequestBody   []byte `json:"request_body"`
	ResponseBody  []byte `json:"response_body"`
	WebSocketBody []byte `json:"websocket_body"`
}

// RecordFilter selects records for SearchRecords. From is an inclusive lower
// bound and To an exclusive upper bound on StartedAt; a zero time means
// "unbounded". KeyID filters on ClientKeyID, with UnknownKeyID selecting the
// records without a key ID. Model matches Model or RequestedModel exactly. Page is
// 1-based and PageSize must be between 1 and MaxPageSize.
type RecordFilter struct {
	From     time.Time
	To       time.Time
	KeyID    string
	Model    string
	Page     int
	PageSize int
}

// RecordPage is one page of records plus the total number of matching records.
type RecordPage struct {
	Items    []Capture `json:"items"`
	Total    int64     `json:"total"`
	Page     int       `json:"page"`
	PageSize int       `json:"page_size"`
}

// KeyOption is one selectable client key. Label is masked by default and never
// contains the full key.
type KeyOption struct {
	ID    string `json:"id"`
	Label string `json:"label"`
}

// RecordOptions is the filter vocabulary of the record browser: the known client
// keys (masked) and the models seen in summaries. A scope whose plaintext key is not
// stored yet is listed as its own option instead of being merged into the unknown
// key, which stays the last option.
type RecordOptions struct {
	Keys   []KeyOption `json:"keys"`
	Models []string    `json:"models"`
}

// RecordDetail is one record together with the raw bodies stored for it.
type RecordDetail struct {
	Record Capture       `json:"record"`
	Bodies CaptureBodies `json:"bodies"`
}

// recordQuery is the driver-neutral form of a validated RecordFilter. A false
// "has" flag means the corresponding SQL bind parameter is NULL, i.e. no filter.
type recordQuery struct {
	FromUS   int64
	HasFrom  bool
	ToUS     int64
	HasTo    bool
	KeyID    string
	HasKey   bool
	Model    string
	HasModel bool
	Limit    int64
	Offset   int64
}

func (q recordQuery) fromParam() sql.NullInt64 {
	return sql.NullInt64{Int64: q.FromUS, Valid: q.HasFrom}
}

func (q recordQuery) toParam() sql.NullInt64 {
	return sql.NullInt64{Int64: q.ToUS, Valid: q.HasTo}
}

// An empty but valid key string explicitly selects the unknown-key group.
func (q recordQuery) keyParam() sql.NullString {
	return sql.NullString{String: q.KeyID, Valid: q.HasKey}
}

func (q recordQuery) modelParam() sql.NullString {
	return sql.NullString{String: q.Model, Valid: q.HasModel}
}

// window is the per-shard scan window that covers the global page. An item at
// global position offset+k has at most offset+k items before it anywhere, hence at
// most that many inside its own shard, so reading offset+limit rows from every
// shard is enough to merge the requested page correctly. The scan itself starts at
// the top of each shard, because the shard-local rank of a global item is unknown.
// SearchRecords refuses a window deeper than MaxSearchWindow for that reason.
func (q recordQuery) window() recordQuery {
	limit := q.Offset + q.Limit
	q.Offset = 0
	q.Limit = limit
	return q
}

// AppendCapture stores one summary together with the raw bodies of that call in a
// single transaction of the shard the capture ID maps to: either the summary and
// the bodies are persisted or nothing is, so a failed append never leaves a
// partial record. A duplicate capture ID fails loudly instead of overwriting the
// stored summary.
//
// When the capture carries both a caller scope and the client key plaintext, the
// key is upserted in the same transaction of that shard (see UpsertClientKey).
// Because the key is a cross-shard mapping - a later capture of the same scope can
// land in any other shard - it is then replicated to every remaining shard. Every
// one of them is attempted even when an earlier one fails, so one broken shard can
// never make the plugin skip the others; the failures are joined and returned as an
// error wrapping ErrKeyIncomplete, because the capture itself is stored. The key is
// therefore eligible for retry on a later usage or capture, and the summary never
// stores the plaintext.
func (s *Store) AppendCapture(ctx context.Context, c Capture, b CaptureBodies) error {
	if ctx == nil {
		ctx = context.Background()
	}
	shards, err := s.active()
	if err != nil {
		return err
	}
	if err := c.validate(); err != nil {
		return fmt.Errorf("storage: append capture: %w", err)
	}
	c.ClientKeyID = normalizeKeyID(c.ClientKeyID)
	id, err := ParseCaptureID(c.CaptureID)
	if err != nil {
		return fmt.Errorf("storage: append capture: %w", err)
	}
	// SQLite and PostgreSQL store the bodies as NOT NULL columns, so a nil slice
	// must be written as an empty (non-NULL) blob.
	b.RequestBody = nonNilBytes(b.RequestBody)
	b.ResponseBody = nonNilBytes(b.ResponseBody)
	b.WebSocketBody = nonNilBytes(b.WebSocketBody)

	target := shards[shardIndex(id, len(shards))]
	if err := appendCaptureToShard(ctx, target, id, c, b); err != nil {
		return err
	}

	if c.ClientKey == "" || c.ClientKeyID == "" {
		return nil
	}
	var keyErrs []error
	for _, other := range shards {
		if other.index == target.index {
			continue
		}
		if err := other.rows.upsertClientKey(ctx, c.ClientKeyID, c.ClientKey); err != nil {
			keyErrs = append(keyErrs, fmt.Errorf("shard %d: %w", other.index, err))
		}
	}
	if len(keyErrs) > 0 {
		return fmt.Errorf("%w: capture %s is stored, the key of scope %s did not reach every shard: %w",
			ErrKeyIncomplete, c.CaptureID, shortKeyID(c.ClientKeyID), errors.Join(keyErrs...))
	}
	return nil
}

// appendCaptureToShard writes the summary, the bodies and the newly learned client
// key of one capture into one shard, in one transaction.
func appendCaptureToShard(ctx context.Context, sh *shard, id []byte, c Capture, b CaptureBodies) error {
	tx, err := sh.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("storage: begin capture %s (shard %d): %w", c.CaptureID, sh.index, err)
	}
	// Rollback after a successful Commit is a no-op that returns ErrTxDone.
	defer func() { _ = tx.Rollback() }()

	txRows := sh.rows.withTx(tx)
	if err := txRows.insertCapture(ctx, id, c); err != nil {
		return fmt.Errorf("storage: append capture %s (shard %d): %w", c.CaptureID, sh.index, err)
	}
	if err := txRows.insertCaptureBodies(ctx, id, b); err != nil {
		return fmt.Errorf("storage: append bodies of capture %s (shard %d): %w", c.CaptureID, sh.index, err)
	}
	if c.ClientKeyID != "" && strings.TrimSpace(c.ClientKey) != "" {
		if err := txRows.upsertClientKey(ctx, c.ClientKeyID, c.ClientKey); err != nil {
			return fmt.Errorf("storage: append capture %s (shard %d): upsert client key of scope %s: %w", c.CaptureID, sh.index, shortKeyID(c.ClientKeyID), err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("storage: commit capture %s (shard %d): %w", c.CaptureID, sh.index, err)
	}
	return nil
}

// UpsertClientKey stores the client key plaintext of one caller scope. Upstream
// request metadata only carries the caller scope, so the plaintext arrives
// separately; this is the only writer of the client_keys table.
//
// The key is a cross-shard mapping: the captures of one scope are spread over every
// shard by capture ID, so the plaintext is written to every shard of the store. A
// shard that fails is reported (the write is idempotent, so the caller can simply
// retry) and never silently skipped, otherwise a shard would keep serving records
// whose scope has no plaintext.
//
// The row is inserted on first sight and updated only when the stored plaintext
// differs, so a repeated upsert is a no-op. No summary is ever read or written
// here: historical captures keep their client_key_id and pick up the current
// plaintext through the join, so a capture is never rewritten.
//
// The scope must be a real one (the "unknown" sentinel and an empty scope are
// refused) and the key must be non-empty.
func (s *Store) UpsertClientKey(ctx context.Context, keyID, clientKey string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	shards, err := s.active()
	if err != nil {
		return err
	}
	keyID = normalizeKeyID(keyID)
	if keyID == "" {
		return errors.New("storage: upsert client key: a caller scope is required")
	}
	if strings.TrimSpace(clientKey) == "" {
		return errors.New("storage: upsert client key: the client key is required")
	}
	var errs []error
	for _, sh := range shards {
		if err := sh.rows.upsertClientKey(ctx, keyID, clientKey); err != nil {
			errs = append(errs, fmt.Errorf("shard %d: %w", sh.index, err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("storage: upsert client key of scope %s: %w", shortKeyID(keyID), errors.Join(errs...))
	}
	return nil
}

// GetRecord returns one summary by capture ID, without its bodies. It reports
// ErrNotFound when no summary has that ID. The returned Capture carries no
// plaintext client key.
func (s *Store) GetRecord(ctx context.Context, captureID string) (Capture, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	sh, id, err := s.locate(ctx, captureID)
	if err != nil {
		return Capture{}, err
	}
	record, err := sh.rows.getCapture(ctx, id)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return Capture{}, fmt.Errorf("%w: capture %s", ErrNotFound, captureID)
	case err != nil:
		return Capture{}, fmt.Errorf("storage: get capture %s (shard %d): %w", captureID, sh.index, err)
	}
	return record, nil
}

// SearchRecords returns one page of summaries, newest first, plus the total number
// of matching records. Ordering is started_at DESC, capture_id DESC, which is total
// because capture_id is unique, so the merged page is deterministic even though the
// records come from several shards.
//
// A store with a single database answers the page with one OFFSET/LIMIT query: the
// offset is pushed down into the database, so a deep page costs no extra memory. A
// sharded store cannot do that - there is no global index and a shard-local rank
// says nothing about the global rank - so it reads at most offset+limit rows from
// every shard, merges them and slices the requested window out of the merge;
// correctness does not depend on any shard holding a contiguous part of the global
// order. That scan materialises up to shards*(offset+limit) summaries, so a window
// deeper than MaxSearchWindow is refused with ErrSearchWindowTooDeep instead of
// being served at an unbounded cost.
func (s *Store) SearchRecords(ctx context.Context, filter RecordFilter) (RecordPage, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	shards, err := s.active()
	if err != nil {
		return RecordPage{}, err
	}
	q, err := filter.query()
	if err != nil {
		return RecordPage{}, err
	}
	if len(shards) == 1 {
		return searchOneShard(ctx, shards[0], q, filter)
	}
	if window := q.Offset + q.Limit; window > MaxSearchWindow {
		return RecordPage{}, fmt.Errorf("%w: offset+limit is %d, the limit for a sharded store is %d: narrow the filters or open the record by capture ID",
			ErrSearchWindowTooDeep, window, MaxSearchWindow)
	}
	scan := q.window()

	prefetch := scan.Limit
	if prefetch > maxMergePrefetch {
		prefetch = maxMergePrefetch
	}
	merged := make([]Capture, 0, int(prefetch)*len(shards))
	var total int64
	for _, sh := range shards {
		count, err := sh.rows.countRecords(ctx, q)
		if err != nil {
			return RecordPage{}, fmt.Errorf("storage: count records (shard %d): %w", sh.index, err)
		}
		total += count
		items, err := sh.rows.listRecords(ctx, scan)
		if err != nil {
			return RecordPage{}, fmt.Errorf("storage: list records (shard %d): %w", sh.index, err)
		}
		merged = append(merged, items...)
	}
	sortRecords(merged)

	items := make([]Capture, 0, int(q.Limit))
	for i := q.Offset; i < int64(len(merged)) && int64(len(items)) < q.Limit; i++ {
		items = append(items, merged[i])
	}
	return RecordPage{Items: items, Total: total, Page: filter.Page, PageSize: filter.PageSize}, nil
}

// searchOneShard answers a page from the only database of the store, with the
// driver's own OFFSET/LIMIT. Nothing is merged, so no window is materialised and no
// depth limit applies: the offset is handled by the database.
func searchOneShard(ctx context.Context, sh *shard, q recordQuery, filter RecordFilter) (RecordPage, error) {
	total, err := sh.rows.countRecords(ctx, q)
	if err != nil {
		return RecordPage{}, fmt.Errorf("storage: count records (shard %d): %w", sh.index, err)
	}
	items, err := sh.rows.listRecords(ctx, q)
	if err != nil {
		return RecordPage{}, fmt.Errorf("storage: list records (shard %d): %w", sh.index, err)
	}
	if items == nil {
		items = []Capture{}
	}
	return RecordPage{Items: items, Total: total, Page: filter.Page, PageSize: filter.PageSize}, nil
}

// sortRecords orders records exactly like the per-shard SQL order:
// started_at_us DESC, capture_id DESC. Comparing the canonical lowercase hex text
// of a capture ID is equivalent to comparing the stored 16-byte value bytewise,
// because every ID has the same length.
func sortRecords(items []Capture) {
	sort.Slice(items, func(i, j int) bool {
		if !items[i].StartedAt.Equal(items[j].StartedAt) {
			return items[i].StartedAt.After(items[j].StartedAt)
		}
		return items[i].CaptureID > items[j].CaptureID
	})
}

// RecordOptions returns the client keys and models that occur in stored
// summaries, for populating the record filter. Key labels are masked; the stored
// plaintext key never leaves this package. A scope with no stored plaintext is
// labelled as unresolved, not as the unknown key. Both lists are merged over every
// shard and deduplicated, so a scope or model that only occurs in one shard is
// still selectable.
func (s *Store) RecordOptions(ctx context.Context) (RecordOptions, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	shards, err := s.active()
	if err != nil {
		return RecordOptions{}, err
	}

	// The rows of one shard arrive ordered by client_key_id; the merge keeps the
	// first non-empty plaintext per scope and re-sorts the union, so the result does
	// not depend on the shard order.
	samples := make(map[string]string)
	unknown := false
	modelSet := make(map[string]struct{})
	for _, sh := range shards {
		keys, err := sh.rows.listRecordKeys(ctx)
		if err != nil {
			return RecordOptions{}, fmt.Errorf("storage: list record keys (shard %d): %w", sh.index, err)
		}
		for _, k := range keys {
			if k.ID == "" {
				unknown = true
				continue
			}
			if sample, seen := samples[k.ID]; !seen || sample == "" {
				samples[k.ID] = k.Key
			}
		}
		models, err := sh.rows.listRecordModels(ctx)
		if err != nil {
			return RecordOptions{}, fmt.Errorf("storage: list record models (shard %d): %w", sh.index, err)
		}
		for _, m := range models {
			modelSet[m] = struct{}{}
		}
	}

	ids := make([]string, 0, len(samples))
	for id := range samples {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	models := make([]string, 0, len(modelSet))
	for m := range modelSet {
		models = append(models, m)
	}
	sort.Strings(models)

	options := RecordOptions{Keys: make([]KeyOption, 0, len(ids)+1), Models: models}
	for _, id := range ids {
		options.Keys = append(options.Keys, KeyOption{ID: id, Label: keyLabel(id, samples[id])})
	}
	if unknown {
		options.Keys = append(options.Keys, KeyOption{ID: UnknownKeyID, Label: "unknown"})
	}
	return options, nil
}

// RecordDetail returns one summary and the raw bodies stored for it. It reports
// ErrNotFound when no summary has that capture ID. The returned Capture carries no
// plaintext client key.
func (s *Store) RecordDetail(ctx context.Context, captureID string) (RecordDetail, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	sh, id, err := s.locate(ctx, captureID)
	if err != nil {
		return RecordDetail{}, err
	}
	record, err := sh.rows.getCapture(ctx, id)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return RecordDetail{}, fmt.Errorf("%w: capture %s", ErrNotFound, captureID)
	case err != nil:
		return RecordDetail{}, fmt.Errorf("storage: get capture %s (shard %d): %w", captureID, sh.index, err)
	}
	bodies, err := sh.rows.getCaptureBodies(ctx, id)
	if err != nil {
		return RecordDetail{}, fmt.Errorf("storage: get bodies of capture %s (shard %d): %w", captureID, sh.index, err)
	}
	return RecordDetail{Record: record, Bodies: bodies}, nil
}

// locate resolves a capture ID to the shard that holds it, so a detail request is a
// primary-key lookup instead of a scan over every shard.
func (s *Store) locate(ctx context.Context, captureID string) (*shard, []byte, error) {
	shards, err := s.active()
	if err != nil {
		return nil, nil, err
	}
	id, err := ParseCaptureID(captureID)
	if err != nil {
		return nil, nil, fmt.Errorf("storage: record: %w", err)
	}
	return shards[shardIndex(id, len(shards))], id, nil
}

// query validates the filter and converts it into bind parameters.
func (f RecordFilter) query() (recordQuery, error) {
	if f.Page < 1 {
		return recordQuery{}, fmt.Errorf("storage: record page must be >= 1 (got %d)", f.Page)
	}
	if f.PageSize < 1 || f.PageSize > MaxPageSize {
		return recordQuery{}, fmt.Errorf("storage: record page size must be between 1 and %d (got %d)", MaxPageSize, f.PageSize)
	}
	if !f.From.IsZero() && !f.To.IsZero() && !f.From.Before(f.To) {
		return recordQuery{}, errors.New("storage: record filter From must be before To (From is inclusive, To is exclusive)")
	}
	// PostgreSQL's generated LIMIT/OFFSET parameters are int32, and the merged scan
	// window of a page is offset+limit. Reject an oversized window consistently on
	// both engines instead of wrapping it.
	offset := (int64(f.Page) - 1) * int64(f.PageSize)
	if offset > (1<<31-1)-int64(f.PageSize) {
		return recordQuery{}, errors.New("storage: record page offset is too large")
	}
	q := recordQuery{
		FromUS:  timeToMicros(f.From),
		HasFrom: !f.From.IsZero(),
		ToUS:    timeToMicros(f.To),
		HasTo:   !f.To.IsZero(),
		KeyID:   strings.TrimSpace(f.KeyID),
		Model:   strings.TrimSpace(f.Model),
		Limit:   int64(f.PageSize),
		Offset:  offset,
	}
	switch q.KeyID {
	case "":
		q.HasKey = false
	case UnknownKeyID:
		// An empty but set key ID selects the records without a key ID.
		q.HasKey, q.KeyID = true, ""
	default:
		q.HasKey = true
	}
	q.HasModel = q.Model != ""
	return q, nil
}

// normalizeKeyID canonicalises a plugin-supplied key ID: the "unknown" sentinel
// and an empty value both mean "no client key metadata" and are stored empty, so
// the unknown group is exactly the rows with an empty client_key_id.
func normalizeKeyID(id string) string {
	id = strings.TrimSpace(id)
	if strings.EqualFold(id, UnknownKeyID) {
		return ""
	}
	return id
}

// validate rejects captures that cannot describe a call. Optional host fields
// (model, requested model, source format, request ID) may be empty. The
// messages never contain the client key.
func (c Capture) validate() error {
	switch {
	case strings.TrimSpace(c.CaptureID) == "":
		return errors.New("CaptureID is required")
	case c.StartedAt.IsZero():
		return errors.New("StartedAt is required")
	case c.CompletedAt.IsZero():
		return errors.New("CompletedAt is required")
	case c.CompletedAt.Before(c.StartedAt):
		return errors.New("CompletedAt must not be before StartedAt")
	case c.EventCount < 0:
		return errors.New("EventCount must not be negative")
	}
	if _, err := ParseCaptureID(c.CaptureID); err != nil {
		return err
	}
	return nil
}

// keyLabel builds the filter label of one scope. A scope whose plaintext was never
// resolved is labelled as such instead of being presented as the unknown key, which
// stays a separate option.
func keyLabel(id, sample string) string {
	short := shortKeyID(id)
	masked := maskClientKey(sample)
	switch {
	case short == "":
		// No scope: the unknown group, labelled by the caller.
		return ""
	case masked == "":
		return unresolvedKeyLabel(short)
	default:
		return masked + " \u00b7 " + short
	}
}

// unresolvedKeyText marks a scope that is known while its plaintext key is not.
const unresolvedKeyText = "\u539f\u6587\u672a\u8bc6\u522b"

// unresolvedKeyLabel labels a scope whose plaintext key has not been resolved yet.
func unresolvedKeyLabel(short string) string {
	if short == "" {
		return unresolvedKeyText
	}
	return unresolvedKeyText + " \u00b7 " + short
}

// shortKeyID truncates a scope for display.
func shortKeyID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// maskClientKey keeps only a short prefix and suffix of the key, so a label can
// identify a key without disclosing it. Keys of 8 characters or less are masked
// completely; the short key ID in the label keeps them distinguishable.
func maskClientKey(key string) string {
	r := []rune(strings.TrimSpace(key))
	switch {
	case len(r) == 0:
		return ""
	case len(r) <= 8:
		return "\u2026"
	case len(r) < 16:
		return string(r[:4]) + "\u2026" + string(r[len(r)-2:])
	default:
		return string(r[:5]) + "\u2026" + string(r[len(r)-3:])
	}
}

// captureFromRow builds a Capture from a summary row. clientKey is only non-empty
// for list rows, which join the client_keys table; it is deliberately not part of
// any JSON representation, and the detail view never selects it.
func captureFromRow(captureID []byte, requestID, clientKey, clientKeyID, model, requestedModel, sourceFormat string, stream bool, startedAtUS, completedAtUS, eventCount int64) Capture {
	return Capture{
		CaptureID:      formatCaptureID(captureID),
		RequestID:      requestID,
		ClientKey:      clientKey,
		ClientKeyID:    clientKeyID,
		Model:          model,
		RequestedModel: requestedModel,
		SourceFormat:   sourceFormat,
		Stream:         stream,
		StartedAt:      microsToTime(startedAtUS),
		CompletedAt:    microsToTime(completedAtUS),
		EventCount:     eventCount,
	}
}
