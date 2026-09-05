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

## CLI 使用

查看已注册任务（包含手动定义的演示任务，以及自动发现的其他任务）

    python -m task_platform.cli.main list

查看任务详情（含 description、生效的 timeout / retry_policy）

    python -m task_platform.cli.main show demo-success

运行任务（SUCCESS / FAILED 两种结果）

    python -m task_platform.cli.main run demo-success
    python -m task_platform.cli.main run demo-failure

取消一个尚未开始运行的任务（CANCELED）

    python -m task_platform.cli.main cancel demo-slow

## 如何新增一个任务（阶段 2 起）

不需要修改任何核心代码（registry.py / executor.py / bootstrap.py / discovery.py），
只需要在 `src/task_platform/jobs/` 目录下新增一个 Python 文件，用 `@job` 装饰器
声明任务：

    # src/task_platform/jobs/my_new_job.py
    from task_platform.jobs.decorators import job

    @job("my.new.task", description="示例任务", timeout_seconds=10.0, max_attempts=2)
    def my_new_task() -> str:
        return "done"

保存后即可通过 CLI 看到并运行它：

    python -m task_platform.cli.main list
    python -m task_platform.cli.main run my-new-task

原理：`bootstrap.build_registry()` 调用 `discover_jobs()`，遍历 `jobs/` 目录下
所有模块并逐个 import；`@job` 装饰器在模块被 import 时把函数注册进
`default_registry`。`build_jobs()` 对于没有显式定义 `Job`（如上面新增的这种情况）
的任务，会依据 handler 上的元数据自动生成一个 `Job` 对象（`job_type` 为
`CUSTOM`），因此新任务无需额外接入代码即可被 CLI 发现、展示、执行。

若任务函数需要访问执行上下文（`execution_id`、专属 `logger`），只需在函数签名
里声明一个名为 `ctx` 的参数，executor 会自动注入：

    @job("my.ctx.task")
    def my_ctx_task(ctx) -> str:
        ctx.logger.info("running with execution_id=%s", ctx.execution_id)
        return "done"

## 项目状态

当前阶段：**阶段 2 - 插件化任务机制**（已完成）

已完成：

- `@job` 装饰器（`jobs/decorators.py`）：把函数注册进模块级 `default_registry`
  单例，可携带 `description` / `timeout_seconds` / `max_attempts` / `backoff` 等元数据
- `TaskRegistry` / `RegisteredHandler` 扩展（`registry.py`）：新增 `timeout_seconds`、
  `retry_policy` 字段，向后兼容阶段 1 手动 `register()` 的调用方式
- `JobContext`（`jobs/context.py`）：统一执行上下文（`execution_id`、专属
  `logger`、预留 `config`），任务函数通过声明 `ctx` 参数按需接收，由
  `call_handler()` 通过 `inspect` 判断是否注入
- `discover_jobs()`（`jobs/discovery.py`）：基于 `pkgutil.iter_modules` +
  `importlib.import_module` 自动发现并 import `jobs/` 目录下的所有模块，
  跳过下划线开头的内部模块
- `demo_jobs.py`（修正自阶段 1 拼写错误的 `dome_jobs.py`），三个演示任务改用
  `@job` 装饰；新增 `example_new_job.py` 作为"新增文件即被发现"的示例
- `executor.py` 新增 `resolve_timeout_seconds()` / `resolve_retry_policy()`：
  以 handler（`@job` 元数据）优先、Job 对象默认值兜底的规则，解析本次执行
  实际生效的超时/重试参数，并记录到 `JobExecution.timeout_seconds` /
  `max_attempts` 上（真正的超时打断与重试循环留给阶段 3 的 Scheduler/Worker）
- `bootstrap.py` 重构：`build_registry()` 改用 `discover_jobs()`；`build_jobs()`
  对没有显式定义的已注册任务自动生成 `Job`（`job_type=CUSTOM`）
- `cli/main.py`：`list`/`show`/`run`/`cancel` 均基于自动发现的结果工作，`show`
  额外展示 description 和生效的 timeout/retry_policy；同时修复了阶段 1 遗留的
  `list` 命令 bug（`return` 误写在循环体内导致只打印第一条任务）
- 全部单元测试通过；`ruff check`、`ruff format --check`、`mypy src` 均无告警

DoD 对照（详见项目蓝图 8.3 / 19 节）：核心调度器/执行器代码无需修改，新增
`jobs/` 目录下的 Python 文件即可被自动发现并执行 —— 已满足（`example_new_job.py`
即为验证用例）。

下一阶段：阶段 3 - 调度器与 Worker（`Scheduler` 主循环、`interval`/简单
`cron` 规则、`ThreadPoolExecutor`、真正的 `timeout`/重试执行、优雅关闭）。
