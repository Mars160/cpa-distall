package plugin

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"

	"github.com/mars160/cpa-distill/internal/storage"
)

// hookEnvelope is the minimal typed view of one hook JSON envelope. Only the
// attributes the plugin actually keeps are read; business payloads are never
// parsed, re-encoded or given new identifiers. Body-bearing fields stay as raw
// JSON here so the caller can decide whether to decode them at all: decoding is
// the only expensive step and is skipped once a capture is known to be over
// budget. Unknown fields (for example upstream OriginalRequest) are ignored.
type hookEnvelope struct {
	RequestID      string          `json:"RequestID"`
	Body           json.RawMessage `json:"Body"`
	ChunkIndex     json.RawMessage `json:"ChunkIndex"`
	Payload        json.RawMessage `json:"Payload"`
	Outcome        json.RawMessage `json:"Outcome"`
	Model          json.RawMessage `json:"Model"`
	RequestedModel json.RawMessage `json:"RequestedModel"`
	SourceFormat   json.RawMessage `json:"SourceFormat"`
	Stream         json.RawMessage `json:"Stream"`
	StartedAt      json.RawMessage `json:"StartedAt"`
	CompletedAt    json.RawMessage `json:"CompletedAt"`
	Metadata       json.RawMessage `json:"Metadata"`
	Headers        json.RawMessage `json:"Headers"`
}

// decodeBodyBytes decodes one ABI []byte field, which the contract carries as a
// base64 JSON string. An absent field or JSON null means "no bytes" and is not an
// error. A present field that is not valid base64 (or not a string at all) is an
// error: the caller must drop the whole capture, because storing the remaining
// bytes would present a truncated body as a complete one.
func decodeBodyBytes(raw json.RawMessage) ([]byte, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	// Fast path: the ABI carries []byte as a plain base64 JSON string, so when
	// there is nothing to unescape the inner text can be decoded in place. This
	// avoids scanning the (potentially large) body with encoding/json a second
	// time; the result is identical for canonical base64.
	if n := len(raw); n >= 2 && raw[0] == '"' && raw[n-1] == '"' {
		content := raw[1 : n-1]
		if bytes.IndexByte(content, '\\') < 0 {
			buf := make([]byte, base64.StdEncoding.DecodedLen(len(content)))
			decoded, err := base64.StdEncoding.Decode(buf, content)
			if err != nil {
				return nil, err
			}
			return buf[:decoded], nil
		}
	}
	// Escaped strings keep exact encoding/json semantics, and JSON null stays
	// valid "no bytes" while a non-string value remains an error.
	var b []byte
	if err := json.Unmarshal(raw, &b); err != nil {
		return nil, err
	}
	return b, nil
}

// decodeChunkIndex reads response.intercept_stream_chunk.ChunkIndex. ok is false
// when the field is absent; a present but non-integer value is an error, because
// the plugin then cannot tell a stream initialisation from real content.
func decodeChunkIndex(raw json.RawMessage) (index int, ok bool, err error) {
	if len(raw) == 0 {
		return 0, false, nil
	}
	if err := json.Unmarshal(raw, &index); err != nil {
		return 0, false, err
	}
	return index, true, nil
}

// decodeText reads an optional string attribute. A malformed value yields "".
func decodeText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) != nil {
		return ""
	}
	return s
}

func decodeBool(raw json.RawMessage) (value, ok bool) {
	if len(raw) == 0 {
		return false, false
	}
	if json.Unmarshal(raw, &value) != nil {
		return false, false
	}
	return value, true
}

// decodeTimestamp reads an optional RFC3339 timestamp; a malformed value is
// ignored rather than aborting the capture.
func decodeTimestamp(raw json.RawMessage) time.Time {
	if len(raw) == 0 {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, decodeText(raw))
	if err != nil {
		return time.Time{}
	}
	return t.UTC()
}

// applySummary folds the pre-parsed typed attributes of one envelope into the
// capture header. It runs under the plugin lock and performs only assignments: the
// JSON work happened in parseSummary, outside the lock. A malformed optional
// attribute is ignored: it never discards body bytes and never clears an attribute
// an earlier event already provided.
func applySummary(c *storage.Capture, s summary, now time.Time) {
	if s.model != "" {
		c.Model = s.model
	}
	if s.requestedModel != "" {
		c.RequestedModel = s.requestedModel
	}
	if s.sourceFormat != "" {
		c.SourceFormat = s.sourceFormat
	}
	if s.hasStream {
		c.Stream = s.stream
	}
	if c.StartedAt.IsZero() {
		c.StartedAt = now
	}
	if !s.startedAt.IsZero() {
		c.StartedAt = s.startedAt
	}
	if !s.completedAt.IsZero() {
		c.CompletedAt = s.completedAt
	}
}

// summary is the decoded, typed view of the optional attributes of one envelope.
// Decoding it is the only work applySummary used to do under the lock.
type summary struct {
	model, requestedModel, sourceFormat string
	stream, hasStream                   bool
	startedAt, completedAt              time.Time
}

// parseSummary decodes the optional attributes of one envelope. It runs outside
// the plugin lock; a malformed value yields the zero value, so it is ignored
// rather than aborting the capture.
func parseSummary(env *hookEnvelope) summary {
	s := summary{
		model:          decodeText(env.Model),
		requestedModel: decodeText(env.RequestedModel),
		sourceFormat:   decodeText(env.SourceFormat),
		startedAt:      decodeTimestamp(env.StartedAt),
		completedAt:    decodeTimestamp(env.CompletedAt),
	}
	s.stream, s.hasStream = decodeBool(env.Stream)
	return s
}

// decodedBodyLen is the number of bytes a base64 body field decodes to, computed
// from the JSON text without decoding it. Base64 padding is accounted for, so the
// value is exact for canonical base64 and never rejects a capture that fits the
// byte budget: the raw JSON length would overestimate by about a third and drop
// data that fits. A field that is absent, null, escaped or not a string yields 0,
// which means "no cheap estimate": the exact limit then applies after decoding,
// so an escaped body is never rejected on its raw length.
func decodedBodyLen(raw json.RawMessage) int {
	n := len(raw)
	if n < 2 || raw[0] != '"' || raw[n-1] != '"' {
		return 0
	}
	content := raw[1 : n-1]
	// A JSON escape means the raw text no longer maps one-to-one onto the decoded
	// bytes, so no length shortcut is safe.
	if bytes.IndexByte(content, '\\') >= 0 {
		return 0
	}
	padding := 0
	if len(content) >= 1 && content[len(content)-1] == '=' {
		padding++
	}
	if len(content) >= 2 && content[len(content)-2] == '=' {
		padding++
	}
	if out := len(content)/4*3 - padding; out > 0 {
		return out
	}
	return 0
}

func keyID(key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		return "unknown"
	}
	// Match the host's session.CallerScope, not usage's unrelated RequestID.
	digest := sha256.Sum256([]byte("cli-proxy-api:caller-scope:v1\x00" + key))
	return hex.EncodeToString(digest[:])
}
