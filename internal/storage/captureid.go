package storage

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

const (
	// CaptureIDHexLen is the canonical text length of a capture ID: 16 random
	// bytes rendered as 32 lowercase hex characters. Every API surface (panel,
	// JSON, scripts) uses this form; the databases store the 16 raw bytes.
	CaptureIDHexLen = 32

	// captureIDBytesLen is the binary length of a capture ID.
	captureIDBytesLen = 16
)

// NewCaptureID returns a fresh capture ID: 128 bits read from crypto/rand and
// rendered as 32 lowercase hex characters. The plugin generates it at the first
// hook of a call, so a capture is identified independently of the host request ID
// and no session concept is involved.
func NewCaptureID() (string, error) {
	var b [captureIDBytesLen]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("storage: generate capture ID: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// ParseCaptureID validates and decodes the canonical text form of a capture ID.
// Uppercase hex is accepted (and compares equal after decoding), any other length
// or alphabet is an error: an ID that does not describe exactly 16 bytes must
// never reach a query or a shard lookup.
func ParseCaptureID(id string) ([]byte, error) {
	id = strings.TrimSpace(id)
	if len(id) != CaptureIDHexLen {
		return nil, fmt.Errorf("CaptureID must be %d hex characters (got %d)", CaptureIDHexLen, len(id))
	}
	raw, err := hex.DecodeString(id)
	if err != nil {
		return nil, errors.New("CaptureID must be 32 hex characters")
	}
	return raw, nil
}

// formatCaptureID renders the stored binary form of a capture ID. The result is
// always lowercase, the canonical text form.
func formatCaptureID(raw []byte) string {
	return hex.EncodeToString(raw)
}
