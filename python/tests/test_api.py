"""
tests/test_api.py — pytest tests for the Python timetravel server.

Tests cover every endpoint for both the v1 and v2 APIs, matching the
behaviour documented in the README and implemented by the Go server.
"""

import json
import sys
import os

import pytest

# Make the python/ package importable when running pytest from the repo root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from server import create_app


@pytest.fixture()
def client():
    """Return a Flask test client backed by an in-memory SQLite database."""
    app = create_app(db_path=":memory:")
    app.config["TESTING"] = True
    with app.test_client() as c:
        yield c


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def post_json(client, url, body):
    return client.post(
        url,
        data=json.dumps(body),
        content_type="application/json",
    )


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------


class TestHealth:
    def test_health(self, client):
        resp = client.post("/api/v1/health")
        assert resp.status_code == 200
        assert resp.get_json() == {"ok": True}


# ---------------------------------------------------------------------------
# V1 — GET /api/v1/records/<id>
# ---------------------------------------------------------------------------


class TestV1GetRecord:
    def test_get_nonexistent_record_returns_400(self, client):
        resp = client.get("/api/v1/records/99")
        assert resp.status_code == 400
        data = resp.get_json()
        assert "error" in data

    def test_get_record_after_create(self, client):
        post_json(client, "/api/v1/records/1", {"hello": "world"})
        resp = client.get("/api/v1/records/1")
        assert resp.status_code == 200
        assert resp.get_json() == {"id": 1, "data": {"hello": "world"}}

    def test_get_invalid_id_string(self, client):
        resp = client.get("/api/v1/records/abc")
        assert resp.status_code == 400

    def test_get_zero_id_returns_400(self, client):
        resp = client.get("/api/v1/records/0")
        assert resp.status_code == 400

    def test_get_negative_id_returns_400(self, client):
        resp = client.get("/api/v1/records/-1")
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# V1 — POST /api/v1/records/<id>  (create)
# ---------------------------------------------------------------------------


class TestV1CreateRecord:
    def test_create_record(self, client):
        resp = post_json(client, "/api/v1/records/1", {"hello": "world"})
        assert resp.status_code == 200
        assert resp.get_json() == {"id": 1, "data": {"hello": "world"}}

    def test_create_ignores_null_values(self, client):
        resp = post_json(client, "/api/v1/records/1", {"a": "1", "b": None})
        assert resp.status_code == 200
        assert resp.get_json() == {"id": 1, "data": {"a": "1"}}

    def test_create_with_invalid_id(self, client):
        resp = post_json(client, "/api/v1/records/0", {"k": "v"})
        assert resp.status_code == 400

    def test_create_with_bad_json(self, client):
        resp = client.post(
            "/api/v1/records/1",
            data="not json",
            content_type="application/json",
        )
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# V1 — POST /api/v1/records/<id>  (update)
# ---------------------------------------------------------------------------


class TestV1UpdateRecord:
    def test_update_existing_record(self, client):
        post_json(client, "/api/v1/records/1", {"hello": "world"})
        resp = post_json(
            client, "/api/v1/records/1", {"hello": "world 2", "status": "ok"}
        )
        assert resp.status_code == 200
        assert resp.get_json() == {
            "id": 1,
            "data": {"hello": "world 2", "status": "ok"},
        }

    def test_delete_field_with_null(self, client):
        post_json(client, "/api/v1/records/1", {"hello": "world", "status": "ok"})
        resp = post_json(client, "/api/v1/records/1", {"hello": None})
        assert resp.status_code == 200
        assert resp.get_json() == {"id": 1, "data": {"status": "ok"}}

    def test_update_reflects_in_get(self, client):
        post_json(client, "/api/v1/records/5", {"x": "1"})
        post_json(client, "/api/v1/records/5", {"x": "2"})
        resp = client.get("/api/v1/records/5")
        assert resp.get_json()["data"] == {"x": "2"}


# ---------------------------------------------------------------------------
# V2 — GET /api/v2/records/<id>
# ---------------------------------------------------------------------------


class TestV2GetRecord:
    def test_get_nonexistent_returns_404(self, client):
        resp = client.get("/api/v2/records/99")
        assert resp.status_code == 404

    def test_get_latest_after_create(self, client):
        post_json(client, "/api/v2/records/10", {"k": "v"})
        resp = client.get("/api/v2/records/10")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["id"] == 10
        assert body["version"] == 1
        assert body["data"] == {"k": "v"}
        assert "created_at" in body

    def test_get_specific_version(self, client):
        post_json(client, "/api/v2/records/10", {"k": "v1"})
        post_json(client, "/api/v2/records/10", {"k": "v2"})
        resp = client.get("/api/v2/records/10?version=1")
        assert resp.status_code == 200
        assert resp.get_json()["data"] == {"k": "v1"}

    def test_get_nonexistent_version_returns_404(self, client):
        post_json(client, "/api/v2/records/10", {"k": "v"})
        resp = client.get("/api/v2/records/10?version=99")
        assert resp.status_code == 404

    def test_get_invalid_version_returns_400(self, client):
        resp = client.get("/api/v2/records/1?version=0")
        assert resp.status_code == 400

    def test_get_invalid_id_returns_400(self, client):
        resp = client.get("/api/v2/records/abc")
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# V2 — GET /api/v2/records/<id>/versions
# ---------------------------------------------------------------------------


class TestV2GetRecordVersions:
    def test_list_versions(self, client):
        post_json(client, "/api/v2/records/20", {"a": "1"})
        post_json(client, "/api/v2/records/20", {"a": "2"})
        post_json(client, "/api/v2/records/20", {"a": "3"})
        resp = client.get("/api/v2/records/20/versions")
        assert resp.status_code == 200
        versions = resp.get_json()
        assert len(versions) == 3
        assert [v["version"] for v in versions] == [1, 2, 3]
        assert versions[0]["data"] == {"a": "1"}
        assert versions[2]["data"] == {"a": "3"}

    def test_list_versions_nonexistent_returns_404(self, client):
        resp = client.get("/api/v2/records/999/versions")
        assert resp.status_code == 404

    def test_list_versions_invalid_id_returns_400(self, client):
        resp = client.get("/api/v2/records/0/versions")
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# V2 — POST /api/v2/records/<id>
# ---------------------------------------------------------------------------


class TestV2PostRecord:
    def test_create_returns_versioned_record(self, client):
        resp = post_json(client, "/api/v2/records/30", {"name": "Alice"})
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["id"] == 30
        assert body["version"] == 1
        assert body["data"] == {"name": "Alice"}
        assert "created_at" in body

    def test_update_increments_version(self, client):
        post_json(client, "/api/v2/records/31", {"name": "Alice"})
        resp = post_json(client, "/api/v2/records/31", {"name": "Bob"})
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["version"] == 2
        assert body["data"] == {"name": "Bob"}

    def test_delete_field_null(self, client):
        post_json(client, "/api/v2/records/32", {"a": "1", "b": "2"})
        resp = post_json(client, "/api/v2/records/32", {"a": None})
        assert resp.status_code == 200
        assert resp.get_json()["data"] == {"b": "2"}

    def test_create_ignores_null_values(self, client):
        resp = post_json(client, "/api/v2/records/33", {"a": "1", "b": None})
        assert resp.status_code == 200
        assert resp.get_json()["data"] == {"a": "1"}

    def test_invalid_id_returns_400(self, client):
        resp = post_json(client, "/api/v2/records/0", {"k": "v"})
        assert resp.status_code == 400

    def test_bad_json_returns_400(self, client):
        resp = client.post(
            "/api/v2/records/1",
            data="not json",
            content_type="application/json",
        )
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Cross-version consistency: v1 writes visible in v2 and vice-versa
# ---------------------------------------------------------------------------


class TestCrossVersionConsistency:
    def test_v1_write_visible_in_v2(self, client):
        post_json(client, "/api/v1/records/100", {"src": "v1"})
        resp = client.get("/api/v2/records/100")
        assert resp.status_code == 200
        assert resp.get_json()["data"] == {"src": "v1"}

    def test_v2_write_visible_in_v1(self, client):
        post_json(client, "/api/v2/records/101", {"src": "v2"})
        resp = client.get("/api/v1/records/101")
        assert resp.status_code == 200
        assert resp.get_json()["data"] == {"src": "v2"}

    def test_v1_update_creates_new_version(self, client):
        post_json(client, "/api/v1/records/102", {"x": "1"})
        post_json(client, "/api/v1/records/102", {"x": "2"})
        versions = client.get("/api/v2/records/102/versions").get_json()
        assert len(versions) == 2
        assert versions[0]["data"] == {"x": "1"}
        assert versions[1]["data"] == {"x": "2"}
