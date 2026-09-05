"""
任务发现：遍历 jobs 包下的所有模块并逐个 import。

设计原则：discover_jobs() 本身对"任务内容"一无所知，它只负责
"把某个包下的所有模块 import 一遍"。真正的注册动作发生在
@job 装饰器里（模块被 import 时，装饰器作为副作用执行 registry.register）。

这正是 DoD 的核心：新增一个 jobs/xxx.py 文件、用 @job 装饰其中的函数，
此模块无需任何修改就能发现并注册它。
"""

from __future__ import annotations

import importlib
import pkgutil
from types import ModuleType


def discover_jobs(package: ModuleType) -> list[str]:
    """import 指定包下的所有直接子模块，返回被 import 的模块全名列表

    Args:
        package: 一个已 import 的包对象，例如 task_platform.jobs
                （必须是包，即要有 __path__，因为需要遍历其目录）

    Notes:
        - 跳过以下划线开头的模块（约定为内部工具模块，不是任务定义文件）
        - 只发现"直接子模块"，不递归子包；如果未来 jobs/ 下需要分子目录
          组织任务，再扩展为 pkgutil.walk_packages
    """
    discovered: list[str] = []
    package_path = package.__path__  # 要求 package 是包而非单个模块
    prefix = f"{package.__name__}."

    for module_info in pkgutil.iter_modules(package_path, prefix):
        module_name = module_info.name
        short_name = module_name.removeprefix(prefix)
        if short_name.startswith("_"):
            continue
        importlib.import_module(module_name)
        discovered.append(module_name)

    return discovered
