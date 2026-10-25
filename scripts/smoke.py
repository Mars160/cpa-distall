#!/usr/bin/env python3
"""Load the real shared library through the documented C ABI; no model credentials needed."""
import base64
import ctypes as c
import json
import hashlib
import pathlib
import sqlite3
import sys
import tempfile


class Buffer(c.Structure):
    _fields_ = [("ptr", c.c_void_p), ("len", c.c_size_t)]


CALL = c.CFUNCTYPE(c.c_int, c.c_char_p, c.c_void_p, c.c_size_t, c.POINTER(Buffer))
FREE = c.CFUNCTYPE(None, c.c_void_p, c.c_size_t)
SHUTDOWN = c.CFUNCTYPE(None)


class Host(c.Structure):
    _fields_ = [("abi_version", c.c_uint32), ("host_ctx", c.c_void_p),
                ("call", c.c_void_p), ("free_buffer", c.c_void_p)]


class API(c.Structure):
    _fields_ = [("abi_version", c.c_uint32), ("call", CALL),
                ("free_buffer", FREE), ("shutdown", SHUTDOWN)]


def main():
    lib = c.CDLL(str(pathlib.Path(sys.argv[1]).resolve()))
    init = lib.cliproxy_plugin_init
    init.argtypes = [c.POINTER(Host), c.POINTER(API)]
    init.restype = c.c_int
    host, api = Host(1, None, None, None), API()
    assert init(c.byref(host), c.byref(api)) == 0
    assert api.abi_version == 1

    def call(method, payload):
        raw = json.dumps(payload).encode()
        request = c.create_string_buffer(raw)
        response = Buffer()
        status = api.call(method.encode(), request, len(raw), c.byref(response))
        try:
            assert status == 0, (method, status)
            result = json.loads(c.string_at(response.ptr, response.len))
            assert result["ok"], result
            if method not in ("plugin.register", "plugin.reconfigure", "management.register", "management.handle"):
                assert result["result"] == {}, "hook must not modify traffic"
            return result
        finally:
            api.free_buffer(response.ptr, response.len)

    b64 = lambda value: base64.b64encode(value).decode()
    with tempfile.TemporaryDirectory(prefix="cpa-distill-smoke-") as directory:
        data = str(pathlib.Path(directory) / "data")
        # writer_count 2 is the sharded SQLite layout: a data directory with a layout
        # manifest and one shard database per writer.
        config = json.dumps({"driver": "sqlite", "dsn": data, "writer_count": 2})  # JSON is valid YAML.
        registration = call("plugin.register", {"config_yaml": b64(config.encode())})
        assert registration["result"]["capabilities"]["request_lifecycle_plugin"]
        assert registration["result"]["schema_version"] == 6
        for field in ("Name", "Version", "Author", "GitHubRepository"):
            assert registration["result"]["metadata"].get(field), "host requires metadata " + field
        capabilities = registration["result"]["capabilities"]
        assert capabilities["management_api"] and capabilities["usage_plugin"]
        declaration = call("management.register", {})["result"]
        assert len(declaration["routes"]) == 4
        assert all(not route.get("Menu") for route in declaration["routes"])
        assert len(declaration["resources"]) == 1

        def management(method, path, body=None):
            response = call("management.handle", {"Method": method, "Path": path,
                "Body": b64(json.dumps(body).encode()) if body is not None else None})["result"]
            assert response["Headers"]["Cache-Control"] == ["no-store"]
            return response["StatusCode"], base64.b64decode(response["Body"])

        status, html = management("GET", "/v0/resource/plugins/cpa-distill/records")
        assert status == 200 and b"__CPA_DISTILL_API_BASE__" not in html
        assert b"/v0/management/plugins/cpa-distill" in html
        assert management("GET", "/v0/resource/plugins/cpa-distill/records/search")[0] == 404
        client_key = "sk-smoke-client-key"
        caller_scope = hashlib.sha256(b"cli-proxy-api:caller-scope:v1\x00" + client_key.encode()).hexdigest()
        for request_id, streaming, outcome in [("ok", False, "succeeded"), ("stream", True, "succeeded"),
                                               ("failed", False, "failed"), ("cancel", True, "canceled")]:
            common = {"RequestID": request_id, "Model": "test-model", "Stream": streaming}
            call("request.intercept_before", {**common, "Body": b64(b'{"messages":[{"role":"user","content":"hello"}]}'),
                                               "Headers": {"Authorization": ["Bearer " + client_key]},
                                               "Metadata": {"caller_scope": caller_scope, "api_key": "never-store"}})
            call("request.intercept_after", {**common, "Body": b64(b"upstream request")})
            if streaming:
                call("response.intercept_stream_chunk", {**common, "ChunkIndex": -1, "Body": None})
                for index, chunk in enumerate([b"data: first\n\n", b"data: second\n\n", b"data: [DONE]\n\n"]):
                    call("response.intercept_stream_chunk", {**common, "ChunkIndex": index, "Body": b64(chunk)})
            elif outcome == "succeeded":
                call("response.intercept_after", {**common, "Body": b64(b"answer\x00\xff")})
            call("request.complete", {**common, "Outcome": outcome})
        # Missing request IDs / malformed shapes must still transparently pass through.
        call("response.intercept_after", {})
        # Reconfigure/drain without closing the new backend, then query via the real ABI.
        refreshed = json.dumps({"driver": "sqlite", "dsn": data, "writer_count": 2, "queue_size": 33})
        call("plugin.reconfigure", {"config_yaml": b64(refreshed.encode())})
        api_base = "/v0/management/plugins/cpa-distill"
        status, raw_page = management("POST", api_base + "/records/search", {
            "page": 1, "page_size": 1, "key_id": caller_scope, "model": "test-model",
            "from": "2010-01-01T00:00:00Z", "to": "2100-01-01T00:00:00Z"})
        page = json.loads(raw_page)
        assert status == 200 and page["total"] == 2 and len(page["items"]) == 1
        assert client_key.encode() not in raw_page
        item = page["items"][0]
        # The record identity is the capture ID alone: the host request ID is kept
        # verbatim for log correlation, and no session concept is exposed at all.
        assert len(item["capture_id"]) == 32 and all(c in "0123456789abcdef" for c in item["capture_id"]), item
        assert item["request_id"] in {"ok", "stream"}, item
        assert "session_id" not in item, item
        status, detail = management("POST", api_base + "/records/detail", {"capture_id": item["capture_id"]})
        detail_body = json.loads(detail)
        assert status == 200 and set(detail_body["bodies"]) == {"request_body", "response_body", "websocket_body"}, detail_body
        assert detail_body["record"]["event_count"] == item["event_count"], detail_body
        assert client_key.encode() not in detail
        status, options = management("GET", api_base + "/records/options")
        assert status == 200 and caller_scope.encode() in options and client_key.encode() not in options
        # The status view reports the writer pool and the physical shard layout.
        status, raw_status = management("GET", api_base + "/status")
        stats = json.loads(raw_status)
        assert status == 200 and stats["writers"] == 2 and stats["shards"] == 2, stats
        call("plugin.shutdown", {})  # Drains the background queue before inspection.
        api.shutdown()  # Idempotent double shutdown, as can happen during unload.
        client_request = b'{"messages":[{"role":"user","content":"hello"}]}'
        # The data directory holds the layout descriptor and one database per writer.
        manifest = json.loads((pathlib.Path(data) / "manifest.json").read_text())
        assert manifest == {"format": "cpa-distill/capture-shards", "version": 1, "writer_count": 2,
                            "shard_algorithm": "sha256-be64-mod-n", "shard_file": "shard-%02d.db"}, manifest
        shards = sorted(pathlib.Path(data).glob("shard-*.db"))
        assert [p.name for p in shards] == ["shard-00.db", "shard-01.db"], shards
        summaries, keys, bodies = [], [], []
        for shard in shards:
            with sqlite3.connect(shard) as conn:
                summaries += conn.execute("SELECT capture_id, request_id, client_key_id FROM captures").fetchall()
                keys += conn.execute("SELECT key_id, client_key FROM client_keys").fetchall()
                bodies += conn.execute("SELECT c.request_id, b.request_body, b.response_body, b.websocket_body "
                                       "FROM capture_bodies b JOIN captures c ON c.capture_id = b.capture_id").fetchall()
        assert {s[1] for s in summaries} == {"ok", "stream"}, summaries
        assert all(s[2] == caller_scope for s in summaries), summaries
        assert len({s[0] for s in summaries}) == 2 and all(len(s[0]) == 16 for s in summaries), summaries
        # The key mapping is cross-shard: every shard holds the caller scope, so any
        # shard can label the records of that scope.
        for shard in shards:
            assert any(k[0] == caller_scope and k[1] == client_key for k in keys), (shard, keys)
        by_id = {b[0]: b[1:] for b in bodies}
        assert set(by_id) == {"ok", "stream"}, by_id
        # request_body 只取客户端首次 before.Body；after 的上游请求正文不保存。
        assert by_id["ok"][0] == client_request, by_id["ok"]
        assert by_id["ok"][1] == b"answer\x00\xff", by_id["ok"]
        assert by_id["stream"][0] == client_request, by_id["stream"]
        assert by_id["stream"][1] == b"data: first\n\ndata: second\n\ndata: [DONE]\n\n", by_id["stream"]
        for _, request_body, response_body, websocket_body in bodies:
            raw = (request_body or b"") + (response_body or b"") + (websocket_body or b"")
            assert client_key.encode() not in raw and b"never-store" not in raw, raw
    print("PASS: C ABI load, transparent hooks, success-only capture, capture_id identity, sharded SQLite layout with cross-shard key mapping, body reconstruction, panel routes, filtered pagination, detail by capture_id, shutdown drain")


if __name__ == "__main__":
    main()
