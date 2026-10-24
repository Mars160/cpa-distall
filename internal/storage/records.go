package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// UnknownKeyID is the KeyOption.ID reported for records whose client key is not
// identified (an empty client_key_id). It is not a sha256 digest, so it can never
// collide with a key ID supplied by the plugin.
const UnknownKeyID = "unknown"

// MaxPageSize is the largest PageSize SearchRecords accepts.
const MaxPageSize = 100

// Capture is the stored summary of one client call. It is the list row of the
// record browser and the header of the record detail view.
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
	SessionID      string    `json:"session_id"`
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

// AppendCapture stores one summary together with the raw bodies of that call in a
// single transaction: either the summary and the bodies are persisted or nothing
// is, so a failed append never leaves a partial record. A duplicate
// (session_id, request_id) fails loudly instead of overwriting the stored summary.
//
// When the capture carries both a caller scope and the client key plaintext, the
// key is upserted in the same transaction (see UpsertClientKey): a failed append
// must not leave a half-written key behind. The summary itself never stores the
// plaintext.
func (s *Store) AppendCapture(ctx context.Context, c Capture, b CaptureBodies) error {
	if ctx == nil {
		ctx = context.Background()
	}
	db, rows, err := s.active()
	if err != nil {
		return err
	}
	if err := c.validate(); err != nil {
		return fmt.Errorf("storage: append capture: %w", err)
	}
	c.ClientKeyID = normalizeKeyID(c.ClientKeyID)
	// SQLite and PostgreSQL store the bodies as NOT NULL columns, so a nil slice
	// must be written as an empty (non-NULL) blob.
	b.RequestBody = nonNilBytes(b.RequestBody)
	b.ResponseBody = nonNilBytes(b.ResponseBody)
	b.WebSocketBody = nonNilBytes(b.WebSocketBody)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("storage: begin capture %s: %w", c.ref(), err)
	}
	// Rollback after a successful Commit is a no-op that returns ErrTxDone.
	defer func() { _ = tx.Rollback() }()

	txRows := rows.withTx(tx)
	if err := txRows.insertCapture(ctx, c); err != nil {
		return fmt.Errorf("storage: append capture %s: %w", c.ref(), err)
	}
	if err := txRows.insertCaptureBodies(ctx, c.SessionID, c.RequestID, b); err != nil {
		return fmt.Errorf("storage: append bodies of capture %s: %w", c.ref(), err)
	}
	if c.ClientKeyID != "" && strings.TrimSpace(c.ClientKey) != "" {
		if err := txRows.upsertClientKey(ctx, c.ClientKeyID, c.ClientKey); err != nil {
			return fmt.Errorf("storage: append capture %s: upsert client key of scope %s: %w", c.ref(), shortKeyID(c.ClientKeyID), err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("storage: commit capture %s: %w", c.ref(), err)
	}
	return nil
}

// UpsertClientKey stores the client key plaintext of one caller scope. Upstream
// request metadata only carries the caller scope, so the plaintext arrives
// separately; this is the only writer of the client_keys table.
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
	_, rows, err := s.active()
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
	if err := rows.upsertClientKey(ctx, keyID, clientKey); err != nil {
		return fmt.Errorf("storage: upsert client key of scope %s: %w", shortKeyID(keyID), err)
	}
	return nil
}

// SearchRecords returns one page of summaries, newest first, plus the total number
// of matching records. Ordering is started_at DESC, session_id DESC,
// request_id DESC, which is total because (session_id, request_id) is unique.
func (s *Store) SearchRecords(ctx context.Context, filter RecordFilter) (RecordPage, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	_, rows, err := s.active()
	if err != nil {
		return RecordPage{}, err
	}
	q, err := filter.query()
	if err != nil {
		return RecordPage{}, err
	}
	total, err := rows.countRecords(ctx, q)
	if err != nil {
		return RecordPage{}, fmt.Errorf("storage: count records: %w", err)
	}
	items, err := rows.listRecords(ctx, q)
	if err != nil {
		return RecordPage{}, fmt.Errorf("storage: list records: %w", err)
	}
	if items == nil {
		items = []Capture{}
	}
	return RecordPage{Items: items, Total: total, Page: filter.Page, PageSize: filter.PageSize}, nil
}

// RecordOptions returns the client keys and models that occur in stored
// summaries, for populating the record filter. Key labels are masked; the stored
// plaintext key never leaves this package. A scope with no stored plaintext is
// labelled as unresolved, not as the unknown key.
func (s *Store) RecordOptions(ctx context.Context) (RecordOptions, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	_, rows, err := s.active()
	if err != nil {
		return RecordOptions{}, err
	}
	keys, err := rows.listRecordKeys(ctx)
	if err != nil {
		return RecordOptions{}, fmt.Errorf("storage: list record keys: %w", err)
	}
	models, err := rows.listRecordModels(ctx)
	if err != nil {
		return RecordOptions{}, fmt.Errorf("storage: list record models: %w", err)
	}

	// Rows arrive ordered by client_key_id; keep the first non-empty plaintext key
	// per scope only to build the masked label. A scope whose rows all have an empty
	// client_key is unresolved: it is real, so it must not be reported as unknown.
	order := make([]string, 0, len(keys))
	samples := make(map[string]string, len(keys))
	unknown := false
	for _, k := range keys {
		if k.ID == "" {
			unknown = true
			continue
		}
		sample, seen := samples[k.ID]
		if !seen {
			order = append(order, k.ID)
			samples[k.ID] = k.Key
			continue
		}
		if sample == "" {
			samples[k.ID] = k.Key
		}
	}

	options := RecordOptions{Keys: make([]KeyOption, 0, len(order)+1), Models: models}
	if options.Models == nil {
		options.Models = []string{}
	}
	for _, id := range order {
		options.Keys = append(options.Keys, KeyOption{ID: id, Label: keyLabel(id, samples[id])})
	}
	if unknown {
		options.Keys = append(options.Keys, KeyOption{ID: UnknownKeyID, Label: "unknown"})
	}
	return options, nil
}

// RecordDetail returns one summary and the raw bodies stored for it. It reports
// ErrNotFound when no summary matches the pair. The returned Capture carries no
// plaintext client key.
func (s *Store) RecordDetail(ctx context.Context, sessionID, requestID string) (RecordDetail, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	_, rows, err := s.active()
	if err != nil {
		return RecordDetail{}, err
	}
	sessionID, requestID = strings.TrimSpace(sessionID), strings.TrimSpace(requestID)
	if sessionID == "" {
		return RecordDetail{}, errors.New("storage: record session ID is required")
	}
	record, err := rows.getCapture(ctx, sessionID, requestID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return RecordDetail{}, fmt.Errorf("%w: capture %s/%s", ErrNotFound, sessionID, requestID)
	case err != nil:
		return RecordDetail{}, fmt.Errorf("storage: get capture %s/%s: %w", sessionID, requestID, err)
	}
	bodies, err := rows.getCaptureBodies(ctx, sessionID, requestID)
	if err != nil {
		return RecordDetail{}, fmt.Errorf("storage: get bodies of capture %s/%s: %w", sessionID, requestID, err)
	}
	return RecordDetail{Record: record, Bodies: bodies}, nil
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
	// PostgreSQL's generated LIMIT/OFFSET parameters are int32. Reject overflow
	// consistently on both engines instead of wrapping a very large page number.
	if int64(f.Page-1) > (1<<31-1)/int64(f.PageSize) {
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
		Offset:  (int64(f.Page) - 1) * int64(f.PageSize),
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
// (model, requested model, source format) may be empty. The messages never contain
// the client key.
func (c Capture) validate() error {
	switch {
	case strings.TrimSpace(c.SessionID) == "":
		return errors.New("SessionID is required")
	case c.StartedAt.IsZero():
		return errors.New("StartedAt is required")
	case c.CompletedAt.IsZero():
		return errors.New("CompletedAt is required")
	case c.CompletedAt.Before(c.StartedAt):
		return errors.New("CompletedAt must not be before StartedAt")
	case c.EventCount < 0:
		return errors.New("EventCount must not be negative")
	}
	return nil
}

// ref identifies a capture in error messages. It never includes the client key.
func (c Capture) ref() string {
	return fmt.Sprintf("%q/%q", c.SessionID, c.RequestID)
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
func captureFromRow(sessionID, requestID, clientKey, clientKeyID, model, requestedModel, sourceFormat string, stream bool, startedAtUS, completedAtUS, eventCount int64) Capture {
	return Capture{
		SessionID:      sessionID,
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
