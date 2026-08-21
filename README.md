# 任务调度与数据处理平台

任务调度与数据处理项目，所有数据均为模拟数据。

## 环境要求
- Python 3.12+
- conda

## 快速开始

创建 conda 环境

    conda create -n task-platform python=3.12 -y
    conda activate task-platform

安装依赖

    pip install pytest ruff mypy pre-commit

初始化仓库

    git init
    pre-commit install

## 开发命令

运行测试

    python -m pytest

代码检查

    ruff check .

类型检查

    mypy src

## CLI 使用（阶段 1）

查看已注册任务

    python -m task_platform.cli.main list

查看任务详情

    python -m task_platform.cli.main show demo-success

运行任务（SUCCESS / FAILED 两种结果）

    python -m task_platform.cli.main run demo-success
    python -m task_platform.cli.main run demo-failure

取消一个尚未开始运行的任务（CANCELED）

    python -m task_platform.cli.main cancel demo-slow

## 项目状态

当前阶段：**阶段 1 - 任务模型与 CLI MVP**（已完成）

已完成：

- 领域模型：`Job`、`JobExecution`、`RetryPolicy`（`src/task_platform/domain/models.py`）及配套枚举（`enums.py`）
- 状态机：合法状态转换控制，非法跳转（如 SUCCESS -> RUNNING）会抛出 `InvalidStateTransitionError`（`state_machine.py`）
- `TaskRegistry`：任务名称到可执行函数的内存注册表（`registry.py`）
- 3 个演示任务：`demo.success` / `demo.failure` / `demo.slow`（`jobs/demo_jobs.py`）
- CLI：`list` / `show` / `run` / `cancel` 四个子命令（`cli/main.py`），执行失败也能通过 `try/except/finally` 正确落状态
- 单元测试 18 个全部通过；`ruff check`、`ruff format --check`、`mypy src` 均无告警

DoD 对照（详见项目蓝图 19 节）：可以注册至少 3 个测试 Job，并能够通过 CLI 观察到 SUCCESS / FAILED / CANCELED 三种状态 —— 已满足。

下一阶段：阶段 2 - 插件化任务机制（`@job` 装饰器 + `importlib` 动态发现）。
