package plugin

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

const maxClientKeyBytes = 4096

type clientKey struct{ id, value string }
type cachedKey struct {
	value string
	seen  time.Time
	size  int
}

// clientIdentity reads the host's verified caller namespace. Upstream auth headers
// must never become client credentials: only before-auth headers whose candidate
// hashes to this exact namespace are eligible.
func clientIdentity(method string, env *hookEnvelope) (string, string) {
	// Absent fields are the common case for most hooks; skip the JSON scan so a
	// missing attribute costs no allocation.
	if len(env.Metadata) == 0 {
		return "unknown", ""
	}
	var metadata struct {
		Scope string `json:"caller_scope"`
	}
	_ = json.Unmarshal(env.Metadata, &metadata)
	scope := strings.ToLower(strings.TrimSpace(metadata.Scope))
	decoded, err := hex.DecodeString(scope)
	if err != nil || len(decoded) != 32 {
		return "unknown", ""
	}
	if method != "request.intercept_before" {
		return scope, ""
	}
	if len(env.Headers) == 0 {
		return scope, ""
	}
	var headers http.Header
	_ = json.Unmarshal(env.Headers, &headers)
	for name, values := range headers {
		for _, value := range values {
			candidate := ""
			switch {
			case strings.EqualFold(name, "Authorization"):
				parts := strings.Fields(value)
				if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
					candidate = parts[1]
				}
			case strings.EqualFold(name, "X-Api-Key"), strings.EqualFold(name, "X-Goog-Api-Key"):
				candidate = strings.TrimSpace(value)
			}
			if candidate != "" && len(candidate) <= maxClientKeyBytes && keyID(candidate) == scope {
				return scope, candidate
			}
		}
	}
	return scope, ""
}

// cacheKey is called with mu held. Keys are bounded separately and also count
// against the total capture budget; the plaintext never becomes a capture body.
func (p *Plugin) cacheKey(id, value string, now time.Time) {
	if old, ok := p.keys[id]; ok {
		old.seen = now
		p.keys[id] = old
		return
	}
	limit := p.current.cfg.MaxBufferBytes / 16
	if limit > 4<<20 {
		limit = 4 << 20
	}
	size := len(id) + len(value) + 128
	if size > limit {
		return
	}
	for len(p.keys) >= 1024 || p.keyBytes+size > limit || p.bytes+p.keyBytes+size > p.current.cfg.MaxBufferBytes {
		oldest := ""
		var seen time.Time
		for k, entry := range p.keys {
			if oldest == "" || entry.seen.Before(seen) {
				oldest, seen = k, entry.seen
			}
		}
		if oldest == "" {
			return
		}
		p.keyBytes -= p.keys[oldest].size
		delete(p.keys, oldest)
	}
	p.keys[id] = cachedKey{value: value, seen: now, size: size}
	p.keyBytes += size
}

func (p *Plugin) cachedClientKey(id string, now time.Time) string {
	entry, ok := p.keys[id]
	if !ok {
		return ""
	}
	if now.Sub(entry.seen) > p.current.cfg.TTL() {
		delete(p.keys, id)
		p.keyBytes -= entry.size
		return ""
	}
	return entry.value
}

func (p *Plugin) clearKeys() { p.keys = make(map[string]cachedKey); p.keyBytes = 0 }

// Usage has an independent RequestID. Correlate only via the host's stable caller
// scope; the store owns the key table, so a key is upserted once per backend and
// historical captures are never rewritten.
//
// A key that is still queued or already written is skipped. A full queue or a
// failed write never marks a key as done, so the next usage of that key retries
// it instead of losing the plaintext forever.
func (p *Plugin) observeUsage(raw []byte) {
	var usage struct {
		APIKey string
		Failed bool
	}
	if json.Unmarshal(raw, &usage) != nil || usage.Failed {
		return
	}
	key := strings.TrimSpace(usage.APIKey)
	if key == "" || len(key) > maxClientKeyBytes {
		return
	}
	id, now := keyID(key), time.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	b := p.current
	if b == nil {
		return
	}
	p.cacheKey(id, key, now)
	if b.keyStored[id] || b.keyPending[id] {
		return
	}
	size := len(id) + len(key) + 256
	if size > b.cfg.MaxBufferBytes-p.bytes-p.keyBytes {
		p.noteDrop()
		return
	}
	select {
	case b.queue <- batch{key: &clientKey{id: id, value: key}, size: size}:
		b.keyPending[id] = true
		p.bytes += size
	default:
		p.noteDrop()
	}
}
