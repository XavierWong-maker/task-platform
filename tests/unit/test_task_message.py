from __future__ import annotations

import pytest

from task_platform.infrastructure.queue.base import InvalidMessageError, TaskMessage


def test_json_roundtrip() -> None:
    msg = TaskMessage("job_1", "run_1", attempt=2, idempotency_key="job_1:2026-10-26:sch_1")
    assert TaskMessage.from_json(msg.to_json()) == msg


def test_defaults_and_stable_json() -> None:
    msg = TaskMessage("job_1", "run_1")
    assert (msg.attempt, msg.idempotency_key) == (1, None)
    assert msg.to_json() == (
        '{"attempt":1,"idempotency_key":null,"job_id":"job_1","run_id":"run_1"}'
    )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"job_id": "", "run_id": "r"},
        {"job_id": "j", "run_id": "  "},
        {"job_id": "j", "run_id": "r", "attempt": 0},
        {"job_id": "j", "run_id": "r", "attempt": True},
        {"job_id": "j", "run_id": "r", "idempotency_key": " "},
    ],
)
def test_invalid_fields_rejected(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        TaskMessage(**kwargs)  # type: ignore[arg-type]


def test_unknown_fields_are_ignored() -> None:
    raw = '{"job_id":"j","run_id":"r","attempt":1,"future_field":"x"}'
    assert TaskMessage.from_json(raw) == TaskMessage("j", "r")


@pytest.mark.parametrize(
    "raw",
    [
        "not json",
        "[1, 2]",
        '{"job_id":"j","run_id":"r"}',  # 缺 attempt
        '{"job_id":"","run_id":"r","attempt":1}',  # 字段不合法
        '{"job_id":"j","run_id":"r","attempt":"1"}',  # 类型不对
    ],
)
def test_malformed_raw_raises_invalid_message(raw: str) -> None:
    with pytest.raises(InvalidMessageError):
        TaskMessage.from_json(raw)
