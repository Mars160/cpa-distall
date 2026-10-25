package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/mars160/cpa-distill/internal/storage/sqlitedb"
	_ "modernc.org/sqlite" // database/sql driver "sqlite"
)

const sqliteDriverName = "sqlite"

// sqlitePragmaParams is applied through the DSN so every connection created by a
// shard pool starts with the right journal mode and busy timeout.
const sqlitePragmaParams = "_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)"

// openSQLite opens the sharded SQLite layout of one plugin instance. The DSN is a
// data directory holding manifest.json and one shard-XX.db per writer; ":memory:"
// selects a throwaway in-memory layout for tests and development. A SQLite URI is
// refused instead of being reinterpreted: the plugin owns the file layout and the
// pragmas.
func openSQLite(ctx context.Context, dsn string, writerCount int) (*Store, error) {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return nil, errors.New("storage: sqlite driver requires a data directory in DSN")
	}
	if strings.HasPrefix(dsn, "file:") {
		return nil, fmt.Errorf("storage: sqlite DSN %q is a SQLite URI, which is not supported; pass a data directory path (or %s) instead, the plugin owns the pragmas and the shard files", dsn, memoryDSN)
	}

	dsns := make([]string, 0, writerCount)
	var lay layout
	if dsn == memoryDSN {
		token, err := newMemoryToken()
		if err != nil {
			return nil, err
		}
		for i := 0; i < writerCount; i++ {
			dsns = append(dsns, memoryShardDSN(token, i))
		}
	} else {
		prepared, err := prepareLayout(dsn, writerCount)
		if err != nil {
			return nil, err
		}
		lay = prepared
		for i := 0; i < writerCount; i++ {
			dsns = append(dsns, sqliteFileDSN(filepath.Join(lay.dir, shardFileName(i))))
		}
	}

	shards := make([]*shard, 0, writerCount)
	for i, shardDSN := range dsns {
		sh, err := openSQLiteShard(ctx, shardDSN, i)
		if err != nil {
			closeShards(shards)
			return nil, err
		}
		shards = append(shards, sh)
	}
	if lay.publish {
		// The layout descriptor is published only after every shard database exists,
		// so a manifest on disk always implies a complete set of shards. A directory
		// that lost one of them is refused on the next open instead of being silently
		// refilled with an empty database.
		if err := publishManifest(lay.dir, newManifest(writerCount)); err != nil {
			closeShards(shards)
			return nil, err
		}
	}
	return &Store{driver: DriverSQLite, writerCount: writerCount, shards: shards}, nil
}

// openSQLiteShard opens one shard database, applies the embedded schema and
// verifies it. A shard pool holds a single connection: it serialises the writer
// goroutine that owns this shard (one shard per writer, several shards per store),
// avoids SQLITE_BUSY storms on its file and keeps an in-memory shard coherent.
// Concurrency comes from the other shard databases, not from this pool.
func openSQLiteShard(ctx context.Context, dsn string, index int) (*shard, error) {
	db, err := sql.Open(sqliteDriverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("storage: open sqlite shard %d: %w", index, err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("storage: ping sqlite shard %d: %w", index, err)
	}
	for _, pragma := range []string{
		"PRAGMA busy_timeout = 5000",
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
	} {
		if _, err := db.ExecContext(ctx, pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("storage: sqlite shard %d %s: %w", index, pragma, err)
		}
	}
	if err := applySchema(ctx, db, DriverSQLite, sqliteSchema); err != nil {
		db.Close()
		// An existing shard from another plugin version is never migrated or dropped,
		// so a schema that cannot be applied is reported as an incompatibility.
		return nil, fmt.Errorf("storage: sqlite shard %d is incompatible with this schema; migrate it manually (existing data is never dropped automatically): %w", index, err)
	}
	if err := verifySchema(ctx, db, DriverSQLite); err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlite shard %d: %w", index, err)
	}
	return &shard{index: index, db: db, rows: sqliteRows{q: sqlitedb.New(db)}}, nil
}

// sqliteFileDSN turns a shard file path into a modernc.org/sqlite file URI
// carrying the pragmas.
func sqliteFileDSN(path string) string {
	return "file:" + escapeSQLitePath(path) + "?" + sqlitePragmaParams
}

// escapeSQLitePath percent-encodes the characters a file URI cannot carry
// literally. It must not escape the separators, so it uses the path form of the URL
// escaper: url.PathEscape would turn every "/" into %2F and produce a URI that
// names a different file.
func escapeSQLitePath(path string) string {
	if !strings.ContainsAny(path, "?#%& \t") {
		return path
	}
	return (&url.URL{Path: path}).EscapedPath()
}

type sqliteRows struct {
	q *sqlitedb.Queries
}

func (r sqliteRows) withTx(tx *sql.Tx) rowStore {
	return sqliteRows{q: r.q.WithTx(tx)}
}

func (r sqliteRows) insertCapture(ctx context.Context, captureID []byte, c Capture) error {
	return r.q.InsertCapture(ctx, sqlitedb.InsertCaptureParams{
		CaptureID:      captureID,
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

func (r sqliteRows) insertCaptureBodies(ctx context.Context, captureID []byte, b CaptureBodies) error {
	return r.q.InsertCaptureBodies(ctx, sqlitedb.InsertCaptureBodiesParams{
		CaptureID:     captureID,
		RequestBody:   b.RequestBody,
		ResponseBody:  b.ResponseBody,
		WebsocketBody: b.WebSocketBody,
	})
}

func (r sqliteRows) getCaptureBodies(ctx context.Context, captureID []byte) (CaptureBodies, error) {
	row, err := r.q.GetCaptureBodies(ctx, captureID)
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

func (r sqliteRows) getCapture(ctx context.Context, captureID []byte) (Capture, error) {
	row, err := r.q.GetCapture(ctx, captureID)
	if err != nil {
		return Capture{}, err
	}
	return captureFromRow(row.CaptureID, row.RequestID, "", row.ClientKeyID, row.Model, row.RequestedModel, row.SourceFormat, row.Stream != 0, row.StartedAtUs, row.CompletedAtUs, row.EventCount), nil
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
		PageLimit:   q.Limit,
	})
	if err != nil {
		return nil, err
	}
	out := make([]Capture, 0, len(rows))
	for _, row := range rows {
		out = append(out, captureFromRow(row.CaptureID, row.RequestID, row.ClientKey, row.ClientKeyID, row.Model, row.RequestedModel, row.SourceFormat, row.Stream != 0, row.StartedAtUs, row.CompletedAtUs, row.EventCount))
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
