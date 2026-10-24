package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/mars160/cpa-distill/internal/storage/sqlitedb"
	_ "modernc.org/sqlite" // database/sql driver "sqlite"
)

const sqliteDriverName = "sqlite"

// sqlitePragmaParams is applied through the DSN so every connection created by
// the pool starts with the right journal mode and busy timeout.
const sqlitePragmaParams = "_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"

func openSQLite(ctx context.Context, dsn string) (*Store, error) {
	path := strings.TrimSpace(dsn)
	if path == "" {
		return nil, errors.New("storage: sqlite driver requires a database path in DSN")
	}
	db, err := sql.Open(sqliteDriverName, sqliteDSN(path))
	if err != nil {
		return nil, fmt.Errorf("storage: open sqlite %q: %w", path, err)
	}

	// One connection serialises writers, avoids SQLITE_BUSY storms on the same
	// file and keeps a ":memory:" database coherent.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("storage: ping sqlite %q: %w", path, err)
	}
	for _, pragma := range []string{
		"PRAGMA busy_timeout = 5000",
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
	} {
		if _, err := db.ExecContext(ctx, pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("storage: sqlite %s: %w", pragma, err)
		}
	}
	if err := applySchema(ctx, db, DriverSQLite, sqliteSchema); err != nil {
		db.Close()
		return nil, err
	}
	if err := verifySchema(ctx, db, DriverSQLite); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db, driver: DriverSQLite, rows: sqliteRows{q: sqlitedb.New(db)}}, nil
}

// sqliteDSN turns a plain path into a modernc.org/sqlite file URI carrying the
// pragmas. An already-parameterised URI is left alone.
func sqliteDSN(path string) string {
	if path == ":memory:" {
		return "file::memory:?_pragma=busy_timeout(5000)"
	}
	if strings.HasPrefix(path, "file:") {
		if strings.Contains(path, "_pragma=") {
			return path
		}
		sep := "?"
		if strings.Contains(path, "?") {
			sep = "&"
		}
		return path + sep + sqlitePragmaParams
	}
	return "file:" + escapeSQLitePath(path) + "?" + sqlitePragmaParams
}

func escapeSQLitePath(path string) string {
	if !strings.ContainsAny(path, "?#%& \t") {
		return path
	}
	return url.PathEscape(path)
}

type sqliteRows struct {
	q *sqlitedb.Queries
}

func (r sqliteRows) withTx(tx *sql.Tx) rowStore {
	return sqliteRows{q: r.q.WithTx(tx)}
}

func (r sqliteRows) insertCapture(ctx context.Context, c Capture) error {
	return r.q.InsertCapture(ctx, sqlitedb.InsertCaptureParams{
		SessionID:      c.SessionID,
		RequestID:      c.RequestID,
		ClientKeyID:    c.ClientKeyID,
		Model:          c.Model,
		RequestedModel: c.RequestedModel,
		SourceFormat:   c.SourceFormat,
		// SQLite has no boolean type: the column is an INTEGER holding 0 or 1.
		Stream:        boolToInt64(c.Stream),
		StartedAtUs:   timeToMicros(c.StartedAt),
		CompletedAtUs: timeToMicros(c.CompletedAt),
		EventCount:    c.EventCount,
	})
}

func (r sqliteRows) insertCaptureBodies(ctx context.Context, sessionID, requestID string, b CaptureBodies) error {
	return r.q.InsertCaptureBodies(ctx, sqlitedb.InsertCaptureBodiesParams{
		SessionID:     sessionID,
		RequestID:     requestID,
		RequestBody:   b.RequestBody,
		ResponseBody:  b.ResponseBody,
		WebsocketBody: b.WebSocketBody,
	})
}

func (r sqliteRows) getCaptureBodies(ctx context.Context, sessionID, requestID string) (CaptureBodies, error) {
	row, err := r.q.GetCaptureBodies(ctx, sqlitedb.GetCaptureBodiesParams{SessionID: sessionID, RequestID: requestID})
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// A summary without bodies is read as empty bodies; the append path always
		// writes both rows in one transaction, so this only covers hand-edited data.
		return CaptureBodies{}, nil
	case err != nil:
		return CaptureBodies{}, err
	}
	return CaptureBodies{RequestBody: row.RequestBody, ResponseBody: row.ResponseBody, WebSocketBody: row.WebsocketBody}, nil
}

func (r sqliteRows) upsertClientKey(ctx context.Context, keyID, clientKey string) error {
	return r.q.UpsertClientKey(ctx, sqlitedb.UpsertClientKeyParams{KeyID: keyID, ClientKey: clientKey})
}

func (r sqliteRows) getCapture(ctx context.Context, sessionID, requestID string) (Capture, error) {
	row, err := r.q.GetCapture(ctx, sqlitedb.GetCaptureParams{SessionID: sessionID, RequestID: requestID})
	if err != nil {
		return Capture{}, err
	}
	return captureFromRow(row.SessionID, row.RequestID, "", row.ClientKeyID, row.Model, row.RequestedModel, row.SourceFormat, row.Stream != 0, row.StartedAtUs, row.CompletedAtUs, row.EventCount), nil
}

func (r sqliteRows) countRecords(ctx context.Context, q recordQuery) (int64, error) {
	return r.q.CountCaptures(ctx, sqlitedb.CountCapturesParams{
		FromUs:      q.fromParam(),
		ToUs:        q.toParam(),
		KeyID:       q.keyParam(),
		FilterModel: q.modelParam(),
	})
}

func (r sqliteRows) listRecords(ctx context.Context, q recordQuery) ([]Capture, error) {
	rows, err := r.q.ListCaptures(ctx, sqlitedb.ListCapturesParams{
		FromUs:      q.fromParam(),
		ToUs:        q.toParam(),
		KeyID:       q.keyParam(),
		FilterModel: q.modelParam(),
		PageOffset:  q.Offset,
		PageSize:    q.Limit,
	})
	if err != nil {
		return nil, err
	}
	out := make([]Capture, 0, len(rows))
	for _, row := range rows {
		out = append(out, captureFromRow(row.SessionID, row.RequestID, row.ClientKey, row.ClientKeyID, row.Model, row.RequestedModel, row.SourceFormat, row.Stream != 0, row.StartedAtUs, row.CompletedAtUs, row.EventCount))
	}
	return out, nil
}

func (r sqliteRows) listRecordKeys(ctx context.Context) ([]keyRow, error) {
	rows, err := r.q.ListCaptureKeys(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]keyRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, keyRow{ID: row.KeyID, Key: row.ClientKey})
	}
	return out, nil
}

func (r sqliteRows) listRecordModels(ctx context.Context) ([]string, error) {
	return r.q.ListCaptureModels(ctx)
}

// boolToInt64 maps a Go boolean onto the 0/1 integer SQLite stores.
func boolToInt64(b bool) int64 {
	if b {
		return 1
	}
	return 0
}
