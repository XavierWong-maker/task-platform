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

运行任务（单次立即执行，会经过超时/重试判定；SUCCESS / RETRYING / FAILED 三种可能结果）

    python -m task_platform.cli.main run demo-success
    python -m task_platform.cli.main run demo-failure

取消一个尚未开始运行的任务（CANCELED）

    python -m task_platform.cli.main cancel demo-slow

启动 Scheduler 主循环（阻塞运行；演示性质：硬编码调度 demo-success 按固定间隔触发；
Ctrl+C 触发优雅关闭——停止接收新任务并等待正在运行的任务收尾后再退出）

    python -m task_platform.cli.main serve
    python -m task_platform.cli.main serve --interval 5 --poll-interval 1

> 注意：`serve` 目前是阶段 3 的占位实现，调度规则是硬编码在 CLI 里的，
> 不支持自定义任意任务的调度；阶段 4 引入持久化后会改为从数据库加载
> 已启用的 (Job, Schedule) 列表。
>
> 另外，Windows 下 `SIGTERM` 不会触发优雅关闭 handler（Python 在 Windows
> 上对 `SIGTERM` 的支持有限），只有 `Ctrl+C`（`SIGINT`）会真正触发；
> Linux 部署时两者都会生效。

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

## 任务执行与重试语义（阶段 3 起）

执行的唯一入口是 `executor.execute_attempt(job, registry, pool, *, attempt_number, run_id=None)`：

- 每次调用代表"一次尝试"，提交到调用方传入的线程池，用
  `Future.result(timeout=...)` 施加超时。超时后无法强制终止正在运行的线程
  （Python 线程不可被强制打断），只能放弃等待，把本次尝试标记为失败，
  `error_message` 中会说明是因超时放弃。
- 是否还能重试由 `RetryPolicy.allows_retry(attempt_number)` 决定：
  还能重试则状态落在 `RETRYING`，用尽则落在 `FAILED`。
- **多次尝试共享同一个 `run_id`**（只有 `attempt` 递增），这样可以通过
  `job_id + run_id` 在日志里串起同一次运行的完整重试链路（对照蓝图第 14 节）。
- `Scheduler.tick()` 负责把"需要重试"这件事变成一次性的堆条目（按
  `RetryPolicy.delay_for_attempt()` 计算延迟后重新入堆），不会在主循环里
  阻塞 `sleep` 等待重试时机，从而不影响其他到期任务的触发。
- 重试链条耗尽或成功后，`Scheduler` 会恢复/推进该任务在其 `Schedule` 上的
  正常触发节奏，不会因为一次失败重试就让任务从调度堆里永久消失。

## 优雅关闭（阶段 3 起）

`Scheduler.run_forever()` 在主线程运行时会注册 `SIGINT`/`SIGTERM` handler：

- 收到信号后立即停止主循环（不再触发新的 tick）
- 等待线程池中已提交、正在执行的任务运行完毕（`ThreadPoolExecutor.shutdown(wait=True)`）
- 之后才真正返回，退出前会恢复原来的信号 handler

`Scheduler.stop()` 提供了同样语义的程序化调用方式（供代码/测试主动触发关闭，
而非依赖真实信号）。

## 项目状态

当前阶段：**阶段 3 - 调度器与 Worker**（已完成）

已完成：

- `domain/enums.py` 新增 `ScheduleType`（`INTERVAL` / `DAILY`）
- `domain/schedule.py`：`Schedule` 领域对象 + `compute_next_run()`，支持固定间隔
  和"每天固定时刻"两种最简单的调度规则
- `scheduler.py`：`Scheduler` 主循环，用 `heapq` 维护"下一次该触发谁"的最小堆，
  避免每轮遍历所有 schedule；`tick()` 单次检查到期任务，`run_forever()`
  提供阻塞式主循环
- `executor.py` 重构：统一为单一入口 `execute_attempt()`（删除了阶段 1/2 遗留的
  `execute()` 以及阶段 3 中间态的 `execute_with_timeout()`），返回
  `AttemptResult(execution, retry_policy, should_retry)`：
  - 通过 `ThreadPoolExecutor` + `Future.result(timeout=...)` 实现超时判定
    （注意：只能放弃等待，不能强制终止线程本身）
  - 通过 `RetryPolicy.allows_retry()` 判断是否还能重试；多次尝试共享同一
    `run_id`，`attempt` 递增
- `Scheduler.tick()` 接入重试语义：失败且允许重试时，按退避策略计算延迟并
  追加一次性重试堆条目；重试耗尽或成功后恢复该任务的常规调度节奏（不会
  让任务从堆里永久消失）
- 优雅关闭：`Scheduler.run_forever()` 注册 `SIGINT`/`SIGTERM` handler，收到信号
  后停止接收新任务、等待在跑任务收尾再退出；`stop()` 提供等价的程序化调用
- CLI 新增 `serve` 子命令，可实际启动 `Scheduler.run_forever()` 并用 Ctrl+C
  手动验证优雅关闭效果（占位实现：硬编码调度 demo-success，阶段 4 后改为从
  数据库加载）
- `cli/main.py` 的 `run` 命令改为调用 `execute_attempt`，临时创建单 worker 线程池
- 新增/更新测试：`test_schedule.py`、`test_scheduler.py`、
  `test_executor_metadata.py`、`test_executor_timeout.py`、
  `test_scheduler_retry.py`、`test_scheduler_shutdown.py`；同步修正了
  `test_executor.py` 中因重试语义变化（第一次失败若还有重试机会，状态是
  `RETRYING` 而非 `FAILED`）而需要更新的断言
- 全部单元测试通过；`ruff check`、`ruff format --check`、`mypy src` 均无告警

DoD 对照（详见项目蓝图 8.4 / 19 节）：可以同时运行多个任务；任务失败按策略
自动重试；程序退出（Ctrl+C）不会留下不可理解的状态，也不会中途丢弃正在
运行的任务 —— 已满足。

下一阶段：阶段 4 - SQLite + SQLAlchemy 持久化（`Job`/`Execution`/`Schedule`/`Log`
持久化，ORM Model + Repository 隔离数据库访问，事务边界，分页/过滤查询，
Alembic 迁移）。
