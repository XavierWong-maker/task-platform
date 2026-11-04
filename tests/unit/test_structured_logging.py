from __future__ import annotations

import io
import json
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from task_platform.cli.main import main
from task_platform.infrastructure.logging.structured import (
    JsonFormatter,
    resolve_log_format,
    resolve_log_level,
    structured_logging,
)


@contextmanager
def _capture(name: str) -> Iterator[tuple[logging.Logger, io.StringIO]]:
    """给指定 logger 挂一个 JSON handler，写入内存；不向上传播，避免影响其他 handler"""
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter("test-svc"))
    logger = logging.getLogger(name)
    previous = (logger.level, logger.propagate)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    try:
        yield logger, stream
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous[0])
        logger.propagate = previous[1]


def _last(stream: io.StringIO) -> dict[str, Any]:
    return json.loads(stream.getvalue().splitlines()[-1])  # type: ignore[no-any-return]


def test_json_line_has_required_fields_and_extras() -> None:
    with _capture("task_platform.test_structured") as (logger, stream):
        logger.info("job started", extra={"job_id": "job_1", "run_id": "run_1", "attempt": 2})

    line = _last(stream)
    assert line["level"] == "INFO"
    assert line["service"] == "test-svc"
    assert line["message"] == "job started"
    assert (line["job_id"], line["run_id"], line["attempt"]) == ("job_1", "run_1", 2)
    assert line["timestamp"].endswith("+00:00")


def test_non_ascii_message_is_not_escaped() -> None:
    with _capture("task_platform.test_structured") as (logger, stream):
        logger.info("任务开始")
    assert "任务开始" in stream.getvalue()


def test_run_id_is_derived_from_job_logger_name() -> None:
    with _capture("task_platform.job.run_abc") as (logger, stream):
        logger.warning("from handler")
    assert _last(stream)["run_id"] == "run_abc"


def test_explicit_run_id_wins_over_logger_name() -> None:
    with _capture("task_platform.job.run_abc") as (logger, stream):
        logger.info("x", extra={"run_id": "run_explicit"})
    assert _last(stream)["run_id"] == "run_explicit"


def test_sensitive_extra_keys_are_redacted() -> None:
    with _capture("task_platform.test_structured") as (logger, stream):
        logger.info("login", extra={"password": "p@ss", "api_token": "abc", "user": "bob"})
    line = _last(stream)
    assert line["password"] == "***" and line["api_token"] == "***"
    assert line["user"] == "bob"
    assert "p@ss" not in stream.getvalue()


def test_exception_is_included_and_unserializable_values_become_strings() -> None:
    with _capture("task_platform.test_structured") as (logger, stream):
        try:
            raise RuntimeError("boom")
        except RuntimeError:
            logger.exception("failed", extra={"obj": object()})
    line = _last(stream)
    assert "RuntimeError: boom" in line["exception"]
    assert isinstance(line["obj"], str)


# ---------------- 配置 ----------------


def test_env_defaults_and_overrides() -> None:
    assert resolve_log_format({}) == "text"
    assert resolve_log_format({"TASK_PLATFORM_LOG_FORMAT": "JSON"}) == "json"
    assert resolve_log_level({}) == logging.INFO
    assert resolve_log_level({"TASK_PLATFORM_LOG_LEVEL": "debug"}) == logging.DEBUG


def test_invalid_env_values_raise() -> None:
    with pytest.raises(ValueError):
        resolve_log_format({"TASK_PLATFORM_LOG_FORMAT": "xml"})
    with pytest.raises(ValueError):
        resolve_log_level({"TASK_PLATFORM_LOG_LEVEL": "LOUD"})


def test_structured_logging_emits_json_to_stderr_and_cleans_up(
    capsys: pytest.CaptureFixture[str],
) -> None:
    logger = logging.getLogger("task_platform")
    handlers_before, level_before = list(logger.handlers), logger.level

    with structured_logging("worker", fmt="json", level=logging.INFO):
        logging.getLogger("task_platform.worker").info("hi", extra={"run_id": "r1"})

    line = json.loads(capsys.readouterr().err.splitlines()[-1])
    assert (line["service"], line["run_id"], line["message"]) == ("worker", "r1", "hi")
    assert logger.handlers == handlers_before and logger.level == level_before


# ---------------- CLI 装配 ----------------


@pytest.fixture
def _db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASK_PLATFORM_DB_URL", f"sqlite:///{tmp_path / 'log_cli.db'}")


@pytest.mark.usefixtures("_db")
def test_cli_run_logs_json_with_job_and_run_id(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("TASK_PLATFORM_LOG_FORMAT", "json")
    assert main(["run", "demo-success"]) == 0

    lines = [json.loads(x) for x in capsys.readouterr().err.splitlines() if x.startswith("{")]
    started = next(x for x in lines if x["message"] == "job attempt started")
    assert {"timestamp", "level", "service", "job_id", "run_id"} <= started.keys()
    assert started["service"] == "cli"


@pytest.mark.usefixtures("_db")
def test_cli_main_leaves_no_handler_behind() -> None:
    logger = logging.getLogger("task_platform")
    handlers_before, level_before = list(logger.handlers), logger.level
    assert main(["list"]) == 0
    assert logger.handlers == handlers_before and logger.level == level_before
