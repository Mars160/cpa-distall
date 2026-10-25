-- Queries shared by both engines for the captures summary, the capture_bodies
-- payload table and the client_keys table.
--
-- capture_id is the primary key of a capture and the only join key: the summary
-- and the bodies of one call always live in the same database (the shard its
-- capture_id maps to) and are written in one transaction. The host request ID is
-- stored verbatim in request_id, a support-only column with neither a uniqueness
-- constraint nor an index, so it is never an identity and never a join key.
--
-- Filters are optional: a NULL bind parameter means "not filtered" (sqlc.narg
-- renders it as sql.NullString / sql.NullInt64). An empty but valid client_key_id
-- means "the rows without a known key", which is how the unknown-key option is
-- queried. Every value travels as a bind parameter; caller input is never
-- concatenated into SQL.
--
-- started_at_us is the range key: from is inclusive, to is exclusive.
-- Ordering is started_at_us DESC, capture_id DESC: it is total because capture_id
-- is the primary key, so the sharded merge is deterministic.

-- name: InsertCapture :exec
INSERT INTO captures (
    capture_id,
    request_id,
    client_key_id,
    model,
    requested_model,
    source_format,
    stream,
    started_at_us,
    completed_at_us,
    event_count
) VALUES (
    sqlc.arg(capture_id),
    sqlc.arg(request_id),
    sqlc.arg(client_key_id),
    sqlc.arg(model),
    sqlc.arg(requested_model),
    sqlc.arg(source_format),
    sqlc.arg(stream),
    sqlc.arg(started_at_us),
    sqlc.arg(completed_at_us),
    sqlc.arg(event_count)
);

-- name: InsertCaptureBodies :exec
INSERT INTO capture_bodies (
    capture_id,
    request_body,
    response_body,
    websocket_body
) VALUES (
    sqlc.arg(capture_id),
    sqlc.arg(request_body),
    sqlc.arg(response_body),
    sqlc.arg(websocket_body)
);

-- name: GetCaptureBodies :one
SELECT
    request_body,
    response_body,
    websocket_body
FROM capture_bodies
WHERE capture_id = sqlc.arg(capture_id);

-- name: UpsertClientKey :exec
-- The key is written only when the stored plaintext differs, so a repeated upsert
-- is a no-op. No capture row is ever read or written here: the summaries keep
-- their client_key_id and the plaintext is joined in on read.
INSERT INTO client_keys (
    key_id,
    client_key
) VALUES (
    sqlc.arg(key_id),
    sqlc.arg(client_key)
)
ON CONFLICT (key_id) DO UPDATE
SET client_key = excluded.client_key
WHERE client_keys.client_key <> excluded.client_key;

-- name: GetCapture :one
SELECT
    capture_id,
    request_id,
    client_key_id,
    model,
    requested_model,
    source_format,
    stream,
    started_at_us,
    completed_at_us,
    event_count
FROM captures
WHERE capture_id = sqlc.arg(capture_id);

-- name: CountCaptures :one
SELECT COUNT(*) AS total
FROM captures
WHERE (CAST(sqlc.narg(from_us) AS BIGINT) IS NULL OR started_at_us >= sqlc.narg(from_us))
  AND (CAST(sqlc.narg(to_us) AS BIGINT) IS NULL OR started_at_us < sqlc.narg(to_us))
  AND (CAST(sqlc.narg(key_id) AS TEXT) IS NULL OR client_key_id = sqlc.narg(key_id))
  AND (
      CAST(sqlc.narg(filter_model) AS TEXT) IS NULL
      OR model = sqlc.narg(filter_model)
      OR requested_model = sqlc.narg(filter_model)
  );

-- name: ListCaptures :many
-- One shard is scanned at most page_offset + page_limit rows deep: the global
-- (offset, limit) window is a subset of the union of the per-shard windows, so
-- the caller merges the shard results and slices the window out of the merge.
-- The list joins client_keys so the caller scope can be labelled without exposing
-- the plaintext through any JSON representation (Capture.ClientKey is json:"-").
SELECT
    c.capture_id,
    c.request_id,
    COALESCE(k.client_key, '') AS client_key,
    c.client_key_id,
    c.model,
    c.requested_model,
    c.source_format,
    c.stream,
    c.started_at_us,
    c.completed_at_us,
    c.event_count
FROM captures c
LEFT JOIN client_keys k ON k.key_id = c.client_key_id
WHERE (CAST(sqlc.narg(from_us) AS BIGINT) IS NULL OR c.started_at_us >= sqlc.narg(from_us))
  AND (CAST(sqlc.narg(to_us) AS BIGINT) IS NULL OR c.started_at_us < sqlc.narg(to_us))
  AND (CAST(sqlc.narg(key_id) AS TEXT) IS NULL OR c.client_key_id = sqlc.narg(key_id))
  AND (
      CAST(sqlc.narg(filter_model) AS TEXT) IS NULL
      OR c.model = sqlc.narg(filter_model)
      OR c.requested_model = sqlc.narg(filter_model)
  )
ORDER BY c.started_at_us DESC, c.capture_id DESC
LIMIT sqlc.arg(page_limit) OFFSET sqlc.arg(page_offset);

-- name: ListCaptureKeys :many
-- Distinct caller scopes seen in the summaries, joined with the plaintext key so a
-- masked label can be built in Go. Only one plaintext exists per key_id; client_key
-- is read here solely for the label and never leaves the storage package in plaintext.
SELECT DISTINCT
    c.client_key_id AS key_id,
    COALESCE(k.client_key, '') AS client_key
FROM captures c
LEFT JOIN client_keys k ON k.key_id = c.client_key_id
ORDER BY c.client_key_id ASC;

-- name: ListCaptureModels :many
SELECT DISTINCT value
FROM (
    SELECT model AS value FROM captures WHERE model <> ''
    UNION
    SELECT requested_model AS value FROM captures WHERE requested_model <> ''
) AS models
ORDER BY value ASC;
