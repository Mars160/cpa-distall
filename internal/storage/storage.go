// Package storage persists CLIProxyAPI hook captures for the distill plugin.
//
// One captured call is stored as three rows in three tables of the same database:
//
//   - captures: the immutable summary (session/request, caller scope, model, stream
//     flag, timing, observed hook count). It backs the record list, the filter
//     options and the detail header. The client key plaintext is never part of it.
//   - capture_bodies: the raw request, response and websocket bytes of that call,
//     opaque and never re-encoded.
//   - client_keys: key_id -> client key plaintext, one row per caller scope. The
//     summary only carries the opaque key_id; the plaintext is joined in when the
//     filter options are built, so upserting a key never rewrites a historical
//     capture.
//
// All three tables are created idempotently on Open; nothing is ever dropped or
// rewritten, and a schema that does not match this plugin version is reported as an
// error instead of being silently migrated.
package storage

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// Driver names accepted by Open.
const (
	DriverSQLite   = "sqlite"
	DriverPostgres = "postgres"
)

// ErrClosed is returned by Store methods called after Close.
var ErrClosed = errors.New("storage: store is closed")

// ErrNotFound is returned when no capture matches the requested session/request.
var ErrNotFound = errors.New("storage: capture not found")

//go:embed schema/sqlite.sql
var sqliteSchema string

//go:embed schema/postgres.sql
var postgresSchema string

// Config selects the driver and its data source name. For the sqlite driver the
// DSN is a database path (":memory:" is accepted); for postgres it is a
// libpq/pgx connection string.
type Config struct {
	Driver string
	DSN    string
}

// Store is a concurrency-safe handle to one captures database. Open owns the
// connection pool and Close releases it.
type Store struct {
	mu     sync.Mutex
	db     *sql.DB
	driver string
	rows   rowStore
	closed bool
}

// rowStore is the driver-specific adapter over the sqlc-generated queries. Every
// method is parameterised: no caller input is ever concatenated into SQL.
type rowStore interface {
	insertCapture(ctx context.Context, c Capture) error
	insertCaptureBodies(ctx context.Context, sessionID, requestID string, b CaptureBodies) error
	getCaptureBodies(ctx context.Context, sessionID, requestID string) (CaptureBodies, error)
	upsertClientKey(ctx context.Context, keyID, clientKey string) error
	getCapture(ctx context.Context, sessionID, requestID string) (Capture, error)
	countRecords(ctx context.Context, q recordQuery) (int64, error)
	listRecords(ctx context.Context, q recordQuery) ([]Capture, error)
	listRecordKeys(ctx context.Context) ([]keyRow, error)
	listRecordModels(ctx context.Context) ([]string, error)
	// withTx rebinds the same queries to an open transaction.
	withTx(tx *sql.Tx) rowStore
}

// keyRow is one distinct (client key ID, client key) pair. The plaintext key is
// only used to build a masked label inside this package.
type keyRow struct {
	ID  string
	Key string
}

// Open connects to the configured database, applies the embedded schema
// idempotently and verifies that the tables match this plugin version. An
// incompatible pre-existing table is reported as an error; it is never dropped or
// rewritten.
func Open(ctx context.Context, cfg Config) (*Store, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	switch driver := strings.ToLower(strings.TrimSpace(cfg.Driver)); driver {
	case DriverSQLite, "sqlite3":
		return openSQLite(ctx, cfg.DSN)
	case DriverPostgres, "postgresql", "pgx":
		return openPostgres(ctx, cfg.DSN)
	case "":
		return nil, errors.New("storage: driver is required (sqlite or postgres)")
	default:
		return nil, fmt.Errorf("storage: unsupported driver %q (want sqlite or postgres)", cfg.Driver)
	}
}

// Driver reports the driver this store was opened with.
func (s *Store) Driver() string {
	if s == nil {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.driver
}

// Close releases the connection pool. It is safe to call more than once.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	db, driver := s.db, s.driver
	s.db, s.rows = nil, nil
	if db == nil {
		return nil
	}
	if err := db.Close(); err != nil {
		return fmt.Errorf("storage: close %s: %w", driver, err)
	}
	return nil
}

func (s *Store) active() (*sql.DB, rowStore, error) {
	if s == nil {
		return nil, nil, ErrClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.db == nil || s.rows == nil {
		return nil, nil, ErrClosed
	}
	return s.db, s.rows, nil
}

// timeToMicros renders a timestamp as UTC epoch microseconds, the storage format
// of the captures table. Sub-microsecond precision is dropped.
func timeToMicros(t time.Time) int64 {
	return t.UTC().UnixMicro()
}

// microsToTime is the inverse of timeToMicros; it always returns a UTC time.
func microsToTime(us int64) time.Time {
	return time.UnixMicro(us).UTC()
}

// nonNilBytes maps a nil body onto an empty non-NULL blob.
func nonNilBytes(b []byte) []byte {
	if b == nil {
		return []byte{}
	}
	return b
}

// applySchema runs the embedded schema statement by statement. The scripts only
// contain plain DDL, so a line-wise "--" comment strip plus a ";" split is enough.
func applySchema(ctx context.Context, db *sql.DB, driver, script string) error {
	for _, stmt := range splitStatements(script) {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("storage: apply %s schema (%s): %w", driver, firstLine(stmt), err)
		}
	}
	return nil
}

// verifySchema fails loudly when an existing table does not match this plugin's
// expectations, instead of silently recreating or altering it.
func verifySchema(ctx context.Context, db *sql.DB, driver string) error {
	for _, check := range []struct {
		table   string
		selects string
	}{
		{"captures", "SELECT session_id, request_id, client_key_id, model, requested_model, source_format, stream, started_at_us, completed_at_us, event_count FROM captures LIMIT 0"},
		{"capture_bodies", "SELECT session_id, request_id, request_body, response_body, websocket_body FROM capture_bodies LIMIT 0"},
		{"client_keys", "SELECT key_id, client_key FROM client_keys LIMIT 0"},
	} {
		rows, err := db.QueryContext(ctx, check.selects)
		if err != nil {
			return fmt.Errorf("storage: %s %s table is incompatible with this schema; migrate it manually (existing data is never dropped automatically): %w", driver, check.table, err)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("storage: %s verify %s table: %w", driver, check.table, err)
		}
	}
	return nil
}

func splitStatements(script string) []string {
	var buf strings.Builder
	for _, line := range strings.Split(script, "\n") {
		if idx := strings.Index(line, "--"); idx >= 0 {
			line = line[:idx]
		}
		if strings.TrimSpace(line) == "" {
			continue
		}
		buf.WriteString(line)
		buf.WriteString("\n")
	}
	var out []string
	for _, stmt := range strings.Split(buf.String(), ";") {
		if s := strings.TrimSpace(stmt); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func firstLine(stmt string) string {
	if idx := strings.IndexByte(stmt, '\n'); idx >= 0 {
		stmt = stmt[:idx]
	}
	return strings.Join(strings.Fields(stmt), " ")
}
