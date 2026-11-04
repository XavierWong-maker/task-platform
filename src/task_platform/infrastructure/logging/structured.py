"""结构化日志：JSON 格式化器 + 进程级日志配置。

每条 JSON 日志至少包含 timestamp / level / service / logger / message；
logging 的 extra 字段（job_id、run_id、attempt ...）展开为顶层键。
配置（环境变量 > 默认值）：
- TASK_PLATFORM_LOG_FORMAT  text | json，默认 text
- TASK_PLATFORM_LOG_LEVEL   DEBUG / INFO / WARNING / ERROR，默认 INFO
"""

from __future__ import annotations

import json
import logging
import os
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

ROOT_LOGGER_NAME = "task_platform"
_JOB_LOGGER_PREFIX = "task_platform.job."
_TEXT_FORMAT = "%(asctime)s %(levelname)s %(message)s"
_VALID_FORMATS = ("text", "json")

# LogRecord 自带的属性：不属于 extra，不进入 JSON 顶层
_STANDARD_ATTRS = frozenset(vars(logging.LogRecord("", 0, "", 0, "", (), None))) | {
    "message",
    "asctime",
}
_SENSITIVE_PARTS = ("password", "passwd", "token", "secret", "authorization", "api_key")
_REDACTED = "***"


def _is_sensitive(key: str) -> bool:
    lowered = key.lower()
    return any(part in lowered for part in _SENSITIVE_PARTS)


class JsonFormatter(logging.Formatter):
    """一条日志 -> 一行 JSON"""

    def __init__(self, service: str = "task-platform") -> None:
        super().__init__()
        self._service = service

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(
                timespec="milliseconds"
            ),
            "level": record.levelname,
            "service": self._service,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in vars(record).items():
            if key in _STANDARD_ATTRS or key.startswith("_") or key in payload:
                continue  # 核心字段不允许被 extra 覆盖
            payload[key] = _REDACTED if _is_sensitive(key) else value

        # JobContext.logger 的名字是 task_platform.job.<run_id>，没有 extra 时从名字还原
        if "run_id" not in payload and record.name.startswith(_JOB_LOGGER_PREFIX):
            payload["run_id"] = record.name.removeprefix(_JOB_LOGGER_PREFIX)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def resolve_log_format(env: dict[str, str] | os._Environ[str] | None = None) -> str:
    source = os.environ if env is None else env
    value = source.get("TASK_PLATFORM_LOG_FORMAT", "text").strip().lower()
    if value not in _VALID_FORMATS:
        raise ValueError(f"TASK_PLATFORM_LOG_FORMAT 必须是 {_VALID_FORMATS} 之一，当前为 {value!r}")
    return value


def resolve_log_level(env: dict[str, str] | os._Environ[str] | None = None) -> int:
    source = os.environ if env is None else env
    name = source.get("TASK_PLATFORM_LOG_LEVEL", "INFO").strip().upper()
    level = logging.getLevelNamesMapping().get(name)
    if level is None:
        raise ValueError(f"TASK_PLATFORM_LOG_LEVEL 不合法：{name!r}")
    return level


@contextmanager
def structured_logging(
    service: str, *, fmt: str | None = None, level: int | None = None
) -> Iterator[logging.Handler]:
    """with 块内给 task_platform logger 挂一个 stderr handler；退出时无条件卸载并还原级别"""
    resolved_fmt = fmt if fmt is not None else resolve_log_format()
    if resolved_fmt not in _VALID_FORMATS:
        raise ValueError(f"fmt 必须是 {_VALID_FORMATS} 之一，当前为 {resolved_fmt!r}")
    resolved_level = level if level is not None else resolve_log_level()

    logger = logging.getLogger(ROOT_LOGGER_NAME)
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(
        JsonFormatter(service) if resolved_fmt == "json" else logging.Formatter(_TEXT_FORMAT)
    )
    previous_level = logger.level
    logger.addHandler(handler)
    logger.setLevel(resolved_level)
    try:
        yield handler
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous_level)
