"""CSV uploads through the driver against a writable backend (GizmoSQL).

Uploads are double-gated: the driver advertises :uploads only when the
connection detail `enable-uploads` is true (writable backends only), and
Metabase's own uploads-settings site setting picks the target database.
Read-only backends (the Spice connection) reject uploads with a clean 422
even if the site setting points at them.
"""
import io
import json
import urllib.error
import urllib.request
import uuid

import pytest

BASE = "http://localhost:3000"

CSV = ("city,temp,happy,visited_on\n"
       "Madrid,31.5,true,2026-07-01\n"
       "Berlin,22.1,false,2026-07-02\n"
       "Caracas,27.9,true,2026-07-03\n")


def multipart_post(api_key, path, file_content, filename="upload.csv", extra_fields=None):
    boundary = uuid.uuid4().hex
    buf = io.BytesIO()
    for k, v in (extra_fields or {}).items():
        buf.write(f"--{boundary}\r\nContent-Disposition: form-data; name=\"{k}\"\r\n\r\n{v}\r\n".encode())
    buf.write(f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; "
              f"filename=\"{filename}\"\r\nContent-Type: text/csv\r\n\r\n".encode())
    buf.write(file_content.encode())
    buf.write(f"\r\n--{boundary}--\r\n".encode())
    r = urllib.request.Request(
        BASE + path, method="POST", data=buf.getvalue(),
        headers={"x-api-key": api_key,
                 "Content-Type": f"multipart/form-data; boundary={boundary}"})
    try:
        with urllib.request.urlopen(r, timeout=300) as resp:
            return resp.status, json.loads(resp.read() or b"null")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or b"null")
        except Exception:
            return e.code, None


@pytest.fixture()
def uploads_on_gizmo(mb):
    """enable-uploads detail on the gizmo connection + uploads-settings
    pointing at it; disables uploads again afterwards."""
    dbs = mb.databases()
    gizmo = dbs["gizmo"]
    if not gizmo["details"].get("enable-uploads"):
        status, _ = mb.req("PUT", f"/api/database/{gizmo['id']}",
                           {"details": {**gizmo["details"], "enable-uploads": True}})
        assert status == 200
    status, _ = mb.req("PUT", "/api/setting/uploads-settings",
                       {"value": {"db_id": gizmo["id"], "schema_name": "main",
                                  "table_prefix": None}})
    assert status in (200, 204)
    yield gizmo["id"]
    mb.req("PUT", "/api/setting/uploads-settings",
           {"value": {"db_id": None, "schema_name": None, "table_prefix": None}})


def test_uploads_feature_gated_by_connection_detail(mb, uploads_on_gizmo):
    dbs = mb.databases()
    _, gizmo_full = mb.get(f"/api/database/{uploads_on_gizmo}")
    assert "uploads" in (gizmo_full.get("features") or [])
    assert "uploads" not in (dbs["flight"].get("features") or []), \
        "read-only backend must not advertise uploads"


def test_upload_query_append_roundtrip(mb, uploads_on_gizmo):
    status, model_id = multipart_post(mb.api_key, "/api/upload/csv", CSV,
                                      filename="weather_e2e.csv",
                                      extra_fields={"collection_id": "root"})
    assert status == 200 and isinstance(model_id, int), (status, model_id)
    table_name = None
    try:
        _, card = mb.get(f"/api/card/{model_id}")
        assert card.get("type") == "model"
        table_id = card["table_id"]
        _, tbl = mb.get(f"/api/table/{table_id}")
        table_name = tbl["name"]
        assert tbl["schema"] == "main"

        # query through the auto-created model
        _, res = mb.post(f"/api/card/{model_id}/query")
        assert res.get("status") == "completed"
        assert len(res["data"]["rows"]) == 3
        madrid = next(r for r in res["data"]["rows"] if r[0] == "Madrid")
        assert madrid[1] == 31.5 and madrid[2] is True

        # query the physical table natively
        res = mb.native(uploads_on_gizmo,
                        f'SELECT COUNT(*) FROM "main"."{table_name}"')
        assert res.get("status") == "completed"
        assert res["data"]["rows"][0][0] == 3

        # append more rows
        status, _ = multipart_post(mb.api_key, f"/api/table/{table_id}/append-csv",
                                   "city,temp,happy,visited_on\nTokyo,29.4,true,2026-07-04\n",
                                   filename="more.csv")
        assert status in (200, 204), status
        res = mb.native(uploads_on_gizmo,
                        f'SELECT COUNT(*) FROM "main"."{table_name}"')
        assert res["data"]["rows"][0][0] == 4
    finally:
        mb.req("DELETE", f"/api/card/{model_id}")
        if table_name:
            mb.native(uploads_on_gizmo, f'DROP TABLE IF EXISTS "main"."{table_name}"')


def test_upload_rejected_on_readonly_backend(mb, uploads_on_gizmo):
    """Even if the site setting points at a non-writable connection, the
    driver's per-connection gate makes Metabase reject the upload cleanly."""
    dbs = mb.databases()
    flight_id = dbs["flight"]["id"]
    gizmo_id = uploads_on_gizmo
    try:
        status, _ = mb.req("PUT", "/api/setting/uploads-settings",
                           {"value": {"db_id": flight_id, "schema_name": "public",
                                      "table_prefix": None}})
        assert status in (200, 204)
        status, resp = multipart_post(mb.api_key, "/api/upload/csv", CSV,
                                      extra_fields={"collection_id": "root"})
        assert status == 422, (status, resp)
        assert "not supported" in str((resp or {}).get("message", "")).lower()
    finally:
        mb.req("PUT", "/api/setting/uploads-settings",
               {"value": {"db_id": gizmo_id, "schema_name": "main",
                          "table_prefix": None}})
