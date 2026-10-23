# 任务调度与数据处理平台

任务调度与数据处理项目，所有数据均为模拟数据。

## 环境要求
- Python 3.12+
- conda

## 快速开始

    conda create -n task-platform python=3.12 -y
    conda activate task-platform
    pip install pytest ruff mypy pre-commit "sqlalchemy>=2.0" "alembic>=1.13" "fastapi>=0.115" "uvicorn>=0.30" httpx2
    git init
    pre-commit install

## 开发命令

    python -m pytest
    ruff check .
    ruff format --check .
    mypy src

## 数据库与迁移

默认数据库：`sqlite:///data/task_platform.db`（目录不存在会自动创建）。
用环境变量 `TASK_PLATFORM_DB_URL` 覆盖（环境变量 > 默认值）。

CLI 每次启动都会自动执行 `alembic upgrade head`。手动操作：

    alembic upgrade head
    alembic downgrade base
    alembic revision --autogenerate -m "describe change"

修改 `infrastructure/db/models.py` 后必须新增迁移，并保证
`tests/integration/test_migrations.py` 通过（它会检查迁移结果与 ORM 模型无差异）。

> 如果本地有阶段 4 早期用 `create_all` 建出来的 `data/task_platform.db`，
> 它没有 `alembic_version` 表，升级会报"表已存在"。开发数据可以直接删掉该文件重建。

### 表结构

```mermaid
erDiagram
    jobs ||--o{ schedules : "job_id (CASCADE)"
    jobs ||--o{ job_executions : "job_id (CASCADE)"
    job_executions ||--o{ job_logs : "(run_id, attempt) (CASCADE)"
    jobs {
        string id PK
        string name UK
        string job_type
        string handler
        float timeout_seconds
        int retry_max_attempts
        string retry_backoff
        bool enabled
        datetime created_at
    }
    schedules {
        string id PK
        string job_id FK
        string schedule_type
        float interval_seconds
        time time_of_day
        bool enabled
    }
    job_executions {
        int id PK
        string run_id "UK(run_id, attempt)"
        int attempt "UK(run_id, attempt)"
        string job_id FK
        string status
        datetime started_at
        datetime finished_at
        text error_message
    }
    job_logs {
        int id PK
        string run_id FK
        int attempt FK
        string level
        text message
        datetime created_at
    }
```

## CLI 使用

    python -m task_platform.cli.main list
    python -m task_platform.cli.main show demo-success
    python -m task_platform.cli.main run demo-success
    python -m task_platform.cli.main cancel demo-slow

给任务添加调度规则（持久化到数据库）：

    python -m task_platform.cli.main schedule demo-success --interval 5
    python -m task_platform.cli.main schedule demo-success --daily 01:00

> `--daily HH:MM` 按 **UTC** 计算，不是本地时区（北京时间 09:00 = UTC 01:00）。

启动 Scheduler（加载数据库中所有"任务已启用且调度已启用"的组合；Ctrl+C 优雅退出）：

    python -m task_platform.cli.main serve
    python -m task_platform.cli.main serve --poll-interval 1

> 没有任何已启用的调度时 `serve` 会直接报错退出。原来的 `serve --interval` 参数已移除，
> 改为先用 `schedule` 添加规则。
>
> Windows 下 `SIGTERM` 不会触发优雅关闭，只有 `Ctrl+C`（`SIGINT`）会；Linux 两者都生效。

查看执行历史（分页、可按任务/状态过滤）和日志：

    python -m task_platform.cli.main history
    python -m task_platform.cli.main history demo-failure --status FAILED --page 1 --page-size 20
    python -m task_platform.cli.main logs <run_id>
    python -m task_platform.cli.main logs <run_id> --attempt 2

### 可靠性（阶段 7）

    python -m task_platform.cli.main recover               # 崩溃恢复（见下方前提）
    python -m task_platform.cli.main resume <run_id>       # 重新运行一个 RECOVERABLE 的运行
    python -m task_platform.cli.main cancel-run <run_id>   # 请求取消一次运行

> `recover` 只应在**确认没有 serve 在运行**时执行：它会把残留的 RUNNING 和悬空的 RETRYING
> 标为 RECOVERABLE，并释放"已认领但从未开始执行"的孤儿幂等键。
>
> 取消是协作式的：RUNNING 的运行由 serve 里的 watcher 在 0.5 秒内转成取消信号，
> 任务需要在循环中调用 `ctx.check_cancelled()` 才能真正停下；不配合的任务在宽限期（默认 5 秒）
> 后被放弃等待并记为 CANCELED（线程可能仍在后台运行）。

故障注入任务（用于演示和测试）：`fault-flaky`（每次运行前 N 次失败）、`fault-slow`（可取消的延迟）、
`fault-hang`（不响应取消）、`fault-ledger`（非幂等的台账写入）。
API：`POST /api/v1/executions/{run_id}/cancel`。

### 重启不丢数据（演示）

    python -m task_platform.cli.main schedule demo-success --interval 3
    python -m task_platform.cli.main serve        # 跑几个周期后 Ctrl+C
    python -m task_platform.cli.main history      # 执行历史还在
    python -m task_platform.cli.main serve        # 再次启动，沿用同一个 Job（id 不变）

## ETL

每日订单 ETL 是注册在平台上的普通任务，可以手动运行或加调度：

    python -m task_platform.cli.main run etl-daily_orders
    python -m task_platform.cli.main schedule etl-daily_orders --daily 01:00

输出在 `data/output/<run_id>/`：`daily_report.csv`、`quality_report.json`、`quarantine.jsonl`。
字段与规则见 `docs/data-dictionary.md`。

可选环境变量（环境变量 > 默认值）：`TASK_PLATFORM_ETL_SEED`、`TASK_PLATFORM_ETL_ORDER_COUNT`
（默认 100000）、`TASK_PLATFORM_ETL_DIRTY_RATIO`（默认 0.03）、`TASK_PLATFORM_ETL_OUTPUT_DIR`。

规模验证：

    python -m task_platform.etl.benchmark --count 1000000 --mode streaming
    python -m task_platform.etl.benchmark --count 1000000 --mode streaming --memory
    python -m task_platform.etl.benchmark --count 1000000 --mode materialized --memory

> `--memory` 会让程序明显变慢，耗时和内存请分开测。

## API

启动服务（启动时自动执行迁移并同步任务定义）：

    uvicorn task_platform.api.app:create_app --factory --reload

交互式文档：`http://127.0.0.1:8000/docs`，OpenAPI：`/openapi.json`。
数据库同样由 `TASK_PLATFORM_DB_URL` 控制。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | /health | 健康检查（含数据库连通性，不可用时 503） |
| POST | /api/v1/jobs | 创建任务（handler 必须已注册；重名 409） |
| GET | /api/v1/jobs | 任务列表，支持 `enabled`、`page`、`page_size` |
| GET | /api/v1/jobs/{job_id} | 任务详情 |
| PATCH | /api/v1/jobs/{job_id} | 局部修改（`name`、`timeout_seconds`、`retry_policy`、`enabled`） |
| DELETE | /api/v1/jobs/{job_id} | 删除任务（级联删除调度与执行记录） |
| POST | /api/v1/jobs/{job_id}/pause | 暂停调度（幂等） |
| POST | /api/v1/jobs/{job_id}/resume | 恢复调度（幂等） |
| POST | /api/v1/jobs/{job_id}/run | 立即运行一次并返回执行记录（同步） |
| GET | /api/v1/executions | 执行记录，支持 `job_id`、`status`、`started_after`、`started_before`、分页 |
| GET | /api/v1/executions/{run_id} | 一次运行及其全部尝试 |
| GET | /api/v1/executions/{run_id}/logs | 运行日志，支持 `attempt`、分页 |

统一错误格式：

    {"error": {"code": "validation_error", "message": "请求参数不合法",
               "details": [{"field": "query.page", "message": "..."}]}}

时间过滤参数必须带时区（如 `2026-10-01T00:00:00Z`）。

Windows PowerShell 调用示例（`curl` 是 `Invoke-WebRequest` 的别名，传 JSON 建议用 `Invoke-RestMethod`）：

    $body = @{ name = "my-job"; handler = "demo.success" } | ConvertTo-Json
    Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8000/api/v1/jobs -ContentType "application/json" -Body $body
    Invoke-RestMethod "http://127.0.0.1:8000/api/v1/jobs?page_size=2"

已知限制见 `docs/adr/0003-api-design.md`（手动触发为同步执行；pause/resume 对已运行的 `serve` 进程需重启生效）。

## 如何新增一个任务

只需在 `src/task_platform/jobs/` 下新增一个 Python 文件，用 `@job` 装饰：

    # src/task_platform/jobs/my_new_job.py
    from task_platform.jobs.decorators import job

    @job("my.new.task", description="示例任务", timeout_seconds=10.0, max_attempts=2)
    def my_new_task() -> str:
        return "done"

下次 CLI 启动时，`discover_jobs()` 会发现它，`sync_jobs_to_db()` 会把它写入数据库
（新任务插入；已存在的任务保留 id/enabled/created_at，handler/timeout/retry 以代码为准）。

需要执行上下文时，在函数签名里声明名为 `ctx` 的参数即可获得 `execution_id` 和专属 `logger`；
`ctx.logger` 的日志会被收集并写入 `job_logs`。

## 执行与重试语义

- 唯一执行入口 `executor.execute_attempt(job, registry, pool, *, attempt_number, run_id=None)`：
  一次调用 = 一次尝试，用 `Future.result(timeout=...)` 判超时（只能放弃等待，不能强杀线程）。
- 失败后是否重试由 `RetryPolicy.allows_retry()` 决定：还能重试落 `RETRYING`，否则 `FAILED`。
- 多次尝试共享同一个 `run_id`，`attempt` 递增；数据库里每次尝试一行，`(run_id, attempt)` 唯一。
- `Scheduler.tick()` 把重试变成一次性堆条目，不阻塞主循环；重试结束后恢复常规排期。
- `Scheduler.run_forever(on_tick=...)` 每轮把触发的 execution 回传给调用方（CLI 在此持久化），
  Scheduler 本身不依赖数据库。

## 项目状态

当前阶段：**阶段 7 - 可靠性与并发控制**（已完成）

阶段 7 交付：
- 幂等键：`idempotency_keys` 表 + 原子认领；Scheduler 按"计划触发时刻"推导键，多实例同槽位只执行一次
- execution 进入 RUNNING 即落库；`recover` / `resume` 处理 RUNNING、悬空 RETRYING、孤儿幂等键
- 协作式取消（`CancelToken` / `ctx.check_cancelled()`）、宽限期、超时触发令牌；
  跨进程取消（`cancel_requested` 列 + watcher）；CLI `cancel-run` 与 API `/cancel`
- 故障注入任务与竞态复现测试（无幂等键重复 N 次 / 有幂等键只执行 1 次）
- 设计记录 `docs/adr/0004-reliability-design.md`（含恢复策略一览与已知限制）

DoD 对照（蓝图 8.8 / 19 节）：

| 项 | 状态 |
| --- | --- |
| 代码：幂等 / 取消 / 恢复 / 故障注入 | 完成 |
| 测试：故障注入、竞态复现与控制 | 完成（`test_race_conditions`、`test_recovery`、`test_cancellation`、`test_executor_cancel`、`test_faults`） |
| 文档：恢复策略 | 完成（ADR-0004） |
| 可运行：故障可恢复 | 完成（见「可靠性」） |

下一阶段：阶段 8 - Redis + Worker 解耦（队列、ack、重复消费、多 Worker）。
