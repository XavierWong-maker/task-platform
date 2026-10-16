from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from task_platform.api.app import create_app


@pytest.fixture
def client(tmp_path: Path) -> Iterator[TestClient]:
    app = create_app(f"sqlite:///{tmp_path / 'api_test.db'}")
    with TestClient(app) as c:  # with 才会触发 lifespan
        yield c


def test_health_ok(client: TestClient) -> None:
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "database": "ok"}


def test_startup_syncs_demo_jobs_into_db(client: TestClient) -> None:
    factory = client.app.state.session_factory  # type: ignore[attr-defined]
    with factory() as s:
        names = {r[0] for r in s.execute(text("SELECT name FROM jobs"))}
    assert {"demo-success", "demo-failure", "demo-slow"} <= names


def test_openapi_docs_available(client: TestClient) -> None:
    assert client.get("/openapi.json").status_code == 200
    assert "/health" in client.get("/openapi.json").json()["paths"]
