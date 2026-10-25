package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver "pgx"
	"github.com/mars160/cpa-distill/internal/storage/postgresdb"
)

const postgresDriverName = "pgx"

// minPostgresConns is the smallest pool the store keeps, so a single-writer
// configuration still has room for the background writers and for the read-only
// management queries of the panel.
const minPostgresConns = 4

// openPostgres opens the single PostgreSQL database of one plugin instance. The
// driver is not sharded: writer_count only sizes the connection pool, so the
// configured number of concurrent writers can really run in parallel on one
// database.
func openPostgres(ctx context.Context, dsn string, writerCount int) (*Store, error) {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return nil, errors.New("storage: postgres driver requires a DSN")
	}
	db, err := sql.Open(postgresDriverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("storage: open postgres: %w", err)
	}
	// One pooled connection per configured writer, never fewer than the minimum.
	conns := writerCount
	if conns < minPostgresConns {
		conns = minPostgresConns
	}
	db.SetMaxOpenConns(conns)
	db.SetMaxIdleConns(conns)
	db.SetConnMaxLifetime(30 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("storage: ping postgres: %w", err)
	}
	if err := applySchema(ctx, db, DriverPostgres, postgresSchema); err != nil {
		db.Close()
		// An existing database from another plugin version is never migrated or
		// dropped, so a schema that cannot be applied is reported as an
		// incompatibility.
		return nil, fmt.Errorf("storage: postgres is incompatible with this schema; migrate it manually (existing data is never dropped automatically): %w", err)
	}
	if err := verifySchema(ctx, db, DriverPostgres); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{
		driver:      DriverPostgres,
		writerCount: writerCount,
		shards:      []*shard{{index: 0, db: db, rows: postgresRows{q: postgresdb.New(db)}}},
	}, nil
}

type postgresRows struct {
	q *postgresdb.Queries
}

func (r postgresRows) withTx(tx *sql.Tx) rowStore {
	return postgresRows{q: r.q.WithTx(tx)}
}

func (r postgresRows) insertCapture(ctx context.Context, captureID []byte, c Capture) error {
	return r.q.InsertCapture(ctx, postgresdb.InsertCaptureParams{
		CaptureID:      captureID,
		RequestID:      c.RequestID,
		ClientKeyID:    c.ClientKeyID,
		Model:          c.Model,
		RequestedModel: c.RequestedModel,
		SourceFormat:   c.SourceFormat,
		Stream:         c.Stream,
		StartedAtUs:    timeToMicros(c.StartedAt),
		CompletedAtUs:  timeToMicros(c.CompletedAt),
		EventCount:     c.EventCount,
	})
}

func (r postgresRows) insertCaptureBodies(ctx context.Context, captureID []byte, b CaptureBodies) error {
	return r.q.InsertCaptureBodies(ctx, postgresdb.InsertCaptureBodiesParams{
		CaptureID:     captureID,
		RequestBody:   b.RequestBody,
		ResponseBody:  b.ResponseBody,
		WebsocketBody: b.WebSocketBody,
	})
}

func (r postgresRows) getCaptureBodies(ctx context.Context, captureID []byte) (CaptureBodies, error) {
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

func (r postgresRows) upsertClientKey(ctx context.Context, keyID, clientKey string) error {
	return r.q.UpsertClientKey(ctx, postgresdb.UpsertClientKeyParams{KeyID: keyID, ClientKey: clientKey})
}

func (r postgresRows) getCapture(ctx context.Context, captureID []byte) (Capture, error) {
	row, err := r.q.GetCapture(ctx, captureID)
	if err != nil {
		return Capture{}, err
	}
	return captureFromRow(row.CaptureID, row.RequestID, "", row.ClientKeyID, row.Model, row.RequestedModel, row.SourceFormat, row.Stream, row.StartedAtUs, row.CompletedAtUs, row.EventCount), nil
}

func (r postgresRows) countRecords(ctx context.Context, q recordQuery) (int64, error) {
	return r.q.CountCaptures(ctx, postgresdb.CountCapturesParams{
		FromUs:      q.fromParam(),
		ToUs:        q.toParam(),
		KeyID:       q.keyParam(),
		FilterModel: q.modelParam(),
	})
}

func (r postgresRows) listRecords(ctx context.Context, q recordQuery) ([]Capture, error) {
	rows, err := r.q.ListCaptures(ctx, postgresdb.ListCapturesParams{
		FromUs:      q.fromParam(),
		ToUs:        q.toParam(),
		KeyID:       q.keyParam(),
		FilterModel: q.modelParam(),
		PageOffset:  int32(q.Offset),
		PageLimit:   int32(q.Limit),
	})
	if err != nil {
		return nil, err
	}
	out := make([]Capture, 0, len(rows))
	for _, row := range rows {
		out = append(out, captureFromRow(row.CaptureID, row.RequestID, row.ClientKey, row.ClientKeyID, row.Model, row.RequestedModel, row.SourceFormat, row.Stream, row.StartedAtUs, row.CompletedAtUs, row.EventCount))
	}
	return out, nil
}

func (r postgresRows) listRecordKeys(ctx context.Context) ([]keyRow, error) {
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

func (r postgresRows) listRecordModels(ctx context.Context) ([]string, error) {
	return r.q.ListCaptureModels(ctx)
}
