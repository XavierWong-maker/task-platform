"""按环境变量构建生产用队列。

TASK_PLATFORM_REDIS_URL     Redis 地址（见 redis_stream.get_redis_url）
TASK_PLATFORM_QUEUE_STREAM  stream 名，默认 task_platform:jobs
"""

from __future__ import annotations

import os

from task_platform.infrastructure.queue.base import TaskQueue
from task_platform.infrastructure.queue.redis_stream import (
    DEFAULT_STREAM,
    RedisStreamQueue,
    create_redis_client,
)


def get_queue_stream() -> str:
    return os.environ.get("TASK_PLATFORM_QUEUE_STREAM", DEFAULT_STREAM)


def build_queue() -> TaskQueue:
    return RedisStreamQueue(create_redis_client(), stream=get_queue_stream())
