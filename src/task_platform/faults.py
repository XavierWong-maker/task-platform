"""故障注入的可复用部件（不是任务：不会被 discover_jobs 发现）"""

from __future__ import annotations

import threading
from collections import defaultdict


class FlakyState:
    """按 run_id 计数：同一次运行的前 fail_times 次调用返回 True（应当失败），之后返回 False。

    同一次运行的各次尝试共享 run_id，所以这能精确复现"失败 N 次后重试成功"，
    不依赖随机数，测试不会偶发失败。状态在进程内存中，进程重启后清零。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._calls: defaultdict[str, int] = defaultdict(int)

    def should_fail(self, run_id: str, fail_times: int) -> bool:
        with self._lock:
            seen = self._calls[run_id]
            self._calls[run_id] = seen + 1
        return seen < fail_times
