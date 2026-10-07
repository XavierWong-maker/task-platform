from __future__ import annotations

import logging

from task_platform.infrastructure.logging.collector import collecting_logs


def test_collects_by_run_id_extra() -> None:
    with collecting_logs() as collector:
        logging.getLogger("task_platform.executor").info("started", extra={"run_id": "run_1"})

    logs = collector.drain("run_1", attempt=2)
    assert [(log.message, log.level, log.attempt) for log in logs] == [("started", "INFO", 2)]


def test_collects_by_job_logger_name() -> None:
    with collecting_logs() as collector:
        logging.getLogger("task_platform.job.run_abc").warning("from handler")

    logs = collector.drain("run_abc", attempt=1)
    assert len(logs) == 1
    assert logs[0].level == "WARNING"


def test_ignores_records_without_run_id() -> None:
    with collecting_logs() as collector:
        logging.getLogger("task_platform.scheduler").info("job triggered")

    assert collector.drain("anything", attempt=1) == []


def test_drain_clears_buffer_and_keeps_runs_separate() -> None:
    with collecting_logs() as collector:
        logger = logging.getLogger("task_platform.executor")
        logger.info("a", extra={"run_id": "run_a"})
        logger.info("b", extra={"run_id": "run_b"})

    assert len(collector.drain("run_a", attempt=1)) == 1
    assert collector.drain("run_a", attempt=1) == []  # 已被取走
    assert len(collector.drain("run_b", attempt=1)) == 1


def test_handler_is_removed_after_context_exits() -> None:
    logger = logging.getLogger("task_platform")
    before = list(logger.handlers)
    with collecting_logs():
        assert len(logger.handlers) == len(before) + 1
    assert logger.handlers == before
