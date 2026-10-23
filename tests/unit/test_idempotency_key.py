from datetime import date

import pytest

from task_platform.domain.idempotency import build_idempotency_key


def test_key_format() -> None:
    assert build_idempotency_key("job_1", date(2026, 10, 16)) == "job_1:2026-10-16"
    assert build_idempotency_key("job_1", date(2026, 10, 16), "p3") == "job_1:2026-10-16:p3"


def test_same_inputs_same_key() -> None:
    assert build_idempotency_key("j", date(2026, 1, 1)) == build_idempotency_key(
        "j", date(2026, 1, 1)
    )


@pytest.mark.parametrize("job_id,partition", [("", None), ("j", ""), ("j", "  ")])
def test_invalid_inputs_rejected(job_id: str, partition: str | None) -> None:
    with pytest.raises(ValueError):
        build_idempotency_key(job_id, date(2026, 1, 1), partition)


def test_too_long_key_rejected() -> None:
    with pytest.raises(ValueError):
        build_idempotency_key("j", date(2026, 1, 1), "x" * 300)
