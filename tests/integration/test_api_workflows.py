from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from task_platform.api.app import create_app

OK_FLOW = [
    {"name": "a", "job": "demo-success"},
    {"name": "b", "job": "demo-success", "depends_on": ["a"]},
]
BAD_FLOW = [
    {"name": "ok", "job": "demo-success"},
    {"name": "bad", "job": "demo-failure", "depends_on": ["ok"]},
]


@pytest.fixture
def client(tmp_path: Path) -> Iterator[TestClient]:
    app = create_app(f"sqlite:///{tmp_path / 'api_wf.db'}")
    with TestClient(app) as c:
        yield c


def _put(client: TestClient, name: str, nodes: list[dict[str, Any]]) -> None:
    resp = client.put(f"/api/v1/workflows/{name}", json={"nodes": nodes})
    assert resp.status_code == 200, resp.text


def _run(client: TestClient, name: str) -> dict[str, Any]:
    resp = client.post(f"/api/v1/workflows/{name}/run")
    assert resp.status_code == 201, resp.text
    return resp.json()  # type: ignore[no-any-return]


# ---------------- 定义 ----------------


def test_definition_crud(client: TestClient) -> None:
    _put(client, "flow", OK_FLOW)
    body = client.get("/api/v1/workflows/flow").json()
    assert body["name"] == "flow"
    assert [n["name"] for n in body["nodes"]] == ["a", "b"]
    assert body["nodes"][1]["depends_on"] == ["a"]
    assert client.get("/api/v1/workflows").json() == ["flow"]

    _put(client, "flow", OK_FLOW[:1])  # 整体替换
    assert len(client.get("/api/v1/workflows/flow").json()["nodes"]) == 1

    assert client.delete("/api/v1/workflows/flow").status_code == 204
    assert client.get("/api/v1/workflows/flow").status_code == 404
    assert client.delete("/api/v1/workflows/flow").status_code == 404


def test_invalid_definitions_return_422(client: TestClient) -> None:
    cyclic = [
        {"name": "a", "job": "demo-success", "depends_on": ["b"]},
        {"name": "b", "job": "demo-success", "depends_on": ["a"]},
    ]
    resp = client.put("/api/v1/workflows/c", json={"nodes": cyclic})
    assert resp.status_code == 422 and resp.json()["error"]["code"] == "workflow_invalid"

    resp = client.put("/api/v1/workflows/e", json={"nodes": []})
    assert resp.status_code == 422 and resp.json()["error"]["code"] == "validation_error"

    resp = client.put("/api/v1/workflows/x", json={"nodes": OK_FLOW, "extra": 1})
    assert resp.status_code == 422


# ---------------- 运行与查询 ----------------


def test_run_returns_and_persists_history(client: TestClient) -> None:
    _put(client, "flow", OK_FLOW)

    body = _run(client, "flow")

    assert body["status"] == "SUCCESS" and body["parent_run_id"] is None
    assert [n["name"] for n in body["nodes"]] == ["a", "b"]
    assert all(n["status"] == "SUCCESS" and n["attempts"] == 1 for n in body["nodes"])
    assert all(not n["reused"] for n in body["nodes"])
    assert client.get(f"/api/v1/workflow-runs/{body['run_id']}").json() == body


def test_list_runs_filters_and_orders_newest_first(client: TestClient) -> None:
    _put(client, "flow", OK_FLOW)
    first = _run(client, "flow")
    second = _run(client, "flow")

    page = client.get("/api/v1/workflow-runs", params={"workflow": "flow"}).json()
    assert page["total"] == 2
    assert [r["run_id"] for r in page["items"]] == [second["run_id"], first["run_id"]]
    assert client.get("/api/v1/workflow-runs", params={"status": "FAILED"}).json()["total"] == 0
    assert client.get("/api/v1/workflow-runs", params={"status": "BOGUS"}).status_code == 422


def test_run_errors(client: TestClient) -> None:
    assert client.post("/api/v1/workflows/nope/run").status_code == 404
    assert client.get("/api/v1/workflow-runs/wfrun_nope").status_code == 404

    _put(client, "ghost", [{"name": "a", "job": "no-such-job"}])
    resp = client.post("/api/v1/workflows/ghost/run")
    assert resp.status_code == 422 and resp.json()["error"]["code"] == "workflow_invalid"


# ---------------- 重试 / 重跑 ----------------


def test_rerun_node_reuses_upstream_results(client: TestClient) -> None:
    _put(client, "flow", OK_FLOW)
    first = _run(client, "flow")

    resp = client.post(f"/api/v1/workflow-runs/{first['run_id']}/rerun", json={"node": "b"})

    assert resp.status_code == 201
    second = resp.json()
    assert second["parent_run_id"] == first["run_id"]
    by_name = {n["name"]: n for n in second["nodes"]}
    assert by_name["a"]["reused"] is True
    assert by_name["a"]["job_run_id"] == first["nodes"][0]["job_run_id"]
    assert by_name["b"]["reused"] is False
    assert second["status"] == "SUCCESS"


def test_replay_errors(client: TestClient) -> None:
    _put(client, "flow", OK_FLOW)
    run = _run(client, "flow")
    base = f"/api/v1/workflow-runs/{run['run_id']}"

    resp = client.post(f"{base}/retry")  # 已全部成功
    assert resp.status_code == 409 and resp.json()["error"]["code"] == "run_not_replayable"
    assert client.post(f"{base}/rerun", json={"node": "zzz"}).status_code == 409
    assert client.post(f"{base}/rerun", json={"node": "a", "extra": 1}).status_code == 422
    assert client.post("/api/v1/workflow-runs/wfrun_nope/retry").status_code == 404
    assert (
        client.post("/api/v1/workflow-runs/wfrun_nope/rerun", json={"node": "a"}).status_code == 404
    )


def test_failed_run_can_be_retried(client: TestClient) -> None:
    _put(client, "flow", BAD_FLOW)
    first = _run(client, "flow")
    assert first["status"] == "PARTIAL_FAILED"

    resp = client.post(f"/api/v1/workflow-runs/{first['run_id']}/retry")

    assert resp.status_code == 201
    second = resp.json()
    assert second["parent_run_id"] == first["run_id"]
    assert second["status"] == "PARTIAL_FAILED"  # demo-failure 仍然失败
    by_name = {n["name"]: n for n in second["nodes"]}
    assert by_name["ok"]["reused"] is True and by_name["bad"]["reused"] is False
