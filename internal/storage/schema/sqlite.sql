-- Idempotent schema for the CLIProxyAPI distill plugin (SQLite engine).
-- Applied on every Open via CREATE ... IF NOT EXISTS; nothing is ever dropped
-- or overwritten, so an existing database keeps its rows untouched.
--
-- The store keeps three tables:
--   captures       one summary row per captured call (the record list / detail header)
--   capture_bodies the raw request/response/websocket bytes of that call
--   client_keys    key_id -> client key plaintext, one row per caller scope
--
-- The client key plaintext is never stored in captures: a summary carries only the
-- opaque client_key_id, and the plaintext is resolved through client_keys when the
-- filter options are built. Upserting a key therefore never rewrites a summary and
-- never touches a historical capture.

CREATE TABLE IF NOT EXISTS captures (
    session_id      TEXT NOT NULL,
    request_id      TEXT NOT NULL,
    client_key_id   TEXT NOT NULL DEFAULT '',
    model           TEXT NOT NULL DEFAULT '',
    requested_model TEXT NOT NULL DEFAULT '',
    source_format   TEXT NOT NULL DEFAULT '',
    stream          INTEGER NOT NULL DEFAULT 0,
    started_at_us   INTEGER NOT NULL,
    completed_at_us INTEGER NOT NULL,
    event_count     INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (session_id, request_id)
);

-- The list is ordered by started_at_us DESC, session_id DESC, request_id DESC and
-- paged with LIMIT/OFFSET, so the sort key is the leading index.
CREATE INDEX IF NOT EXISTS captures_started_at_us_session_id_request_id_idx
    ON captures (started_at_us, session_id, request_id);

-- The key filter probes one caller scope over a time range.
CREATE INDEX IF NOT EXISTS captures_client_key_id_started_at_us_idx
    ON captures (client_key_id, started_at_us);

-- Bodies live in their own table so the summary rows (the record list) stay small
-- and a list scan never touches the payload bytes. The primary key is the only
-- index: bodies are always fetched by the same (session_id, request_id) pair.
CREATE TABLE IF NOT EXISTS capture_bodies (
    session_id     TEXT NOT NULL,
    request_id     TEXT NOT NULL,
    request_body   BLOB NOT NULL,
    response_body  BLOB NOT NULL,
    websocket_body BLOB NOT NULL,
    PRIMARY KEY (session_id, request_id)
);

-- One row per caller scope. key_id is an opaque identifier supplied by the plugin
-- (sha256 hex); an empty key_id means "no client key metadata" and is never stored
-- here, so the unknown group stays exactly the summaries with an empty
-- client_key_id. The primary key is the only index: keys are always read by id.
CREATE TABLE IF NOT EXISTS client_keys (
    key_id     TEXT NOT NULL PRIMARY KEY,
    client_key TEXT NOT NULL DEFAULT ''
);
