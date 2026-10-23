"""取消的服务层：请求取消（写数据库）+ watcher（把数据库里的请求转成本进程令牌）"""

from __future__ import annotations

import logging
import threading
from enum import StrEnum

from sqlalchemy.orm import Session, sessionmaker

from task_platform.domain.cancel import ActiveRuns
from task_platform.domain.enums import JobStatus
from task_platform.domain.models import utc_now
from task_platform.domain.state_machine import transition
from task_platform.infrastructure.db.repositories import ExecutionRepository
from task_platform.infrastructure.db.session import session_scope

logger = logging.getLogger("task_platform.cancellation")


class CancelOutcome(StrEnum):
    REQUESTED = "REQUESTED"  # RUNNING：已置标志，由执行它的进程协作式停止
    CANCELED = "CANCELED"  # RECOVERABLE：已直接取消
    ALREADY_FINISHED = "ALREADY_FINISHED"
    NOT_FOUND = "NOT_FOUND"
    NOT_CANCELLABLE = "NOT_CANCELLABLE"  # 例如 RETRYING：重试排期只在 serve 内存里


def request_cancel(session: Session, run_id: str) -> CancelOutcome:
    repo = ExecutionRepository(session)
    attempts = repo.list_by_run(run_id)
    if not attempts:
        return CancelOutcome.NOT_FOUND

    last = attempts[-1]
    if last.status is JobStatus.RUNNING:
        repo.mark_cancel_requested(run_id, last.attempt)
        return CancelOutcome.REQUESTED
    if last.status is JobStatus.RECOVERABLE:
        transition(last, JobStatus.CANCELED)
        last.finished_at = utc_now()
        repo.update(last)
        return CancelOutcome.CANCELED
    if last.status.is_terminal():
        return CancelOutcome.ALREADY_FINISHED
    return CancelOutcome.NOT_CANCELLABLE


class CancelWatcher:
    """后台线程：定期查询本进程活跃 run 在数据库里是否被请求取消，是则触发令牌"""

    def __init__(
        self, factory: sessionmaker[Session], active: ActiveRuns, *, interval: float = 0.5
    ) -> None:
        self._factory = factory
        self._active = active
        self._interval = interval
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, name="cancel-watcher", daemon=True)

    def poll_once(self) -> list[str]:
        ids = self._active.active_ids()
        if not ids:
            return []
        with session_scope(self._factory) as s:
            requested = ExecutionRepository(s).list_cancel_requested(ids)
        for run_id in requested:
            self._active.cancel(run_id)
            logger.info("cancel requested, token fired", extra={"run_id": run_id})
        return requested

    def _loop(self) -> None:
        while not self._stop.wait(self._interval):
            try:
                self.poll_once()
            except Exception:
                logger.exception("cancel watcher poll failed")

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)
