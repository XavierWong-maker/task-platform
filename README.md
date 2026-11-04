# 任务调度与数据处理平台

任务调度与数据处理项目，所有数据均为模拟数据。

## 环境要求
- Python 3.12+
- conda

## 快速开始

    conda create -n task-platform python=3.12 -y
    conda activate task-platform
    pip install pytest ruff mypy pre-commit "sqlalchemy>=2.0" "alembic>=1.13" "fastapi>=0.115" "uvicorn>=0.30" "redis>=5.0" httpx2
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

## 队列模式（阶段 8）

调度与执行分离：`dispatch` 只负责按调度规则向 Redis 投递任务，`worker` 负责消费并执行，可以同时启动多个。

    dispatch --(Redis Streams)--> worker × N --> SQLite（执行记录/日志）
        |                                              ^
        +-- 到期重试：扫描 next_retry_at 并投递 ----------+

### 启动 Redis（Docker）

    docker run -d --name task-platform-redis -p 6379:6379 -v task-platform-redis-data:/data redis:7-alpine redis-server --appendonly yes
    docker exec -it task-platform-redis redis-cli ping    # 返回 PONG

之后用 `docker stop/start task-platform-redis` 启停。如果 6379 被占用，把 `-p` 的左边改成别的端口（例如 `16379:6379`），并同步修改下面的 URL。

环境变量（环境变量 > 默认值）。**每个新终端都要重新设置**：

    $env:TASK_PLATFORM_REDIS_URL = "redis://localhost:6379/0"   # 默认值
    $env:TASK_PLATFORM_QUEUE_STREAM = "task_platform:jobs"      # 默认值，可不设

### 命令

    python -m task_platform.cli.main schedule demo-success --interval 5
    python -m task_platform.cli.main dispatch                   # 终端 1
    python -m task_platform.cli.main worker                     # 终端 2，可再开多个

    worker 参数：
      --consumer NAME         消费者名，默认 <主机名>-<pid>
      --block-seconds N       队列为空时每次阻塞等待的秒数（默认 1）
      --stale-after N         pending 消息空闲超过 N 秒视为原 Worker 已崩溃并被接管（默认 60，0 表示不接管）
      --heartbeat-interval N  心跳续约间隔，默认 stale-after / 3

> **所有 Worker 必须使用相同的 `--stale-after`**，且每个 Worker 的心跳间隔都要小于最小的 `stale-after`，
> 否则配置较短的 Worker 会抢走别人仍在执行的消息，造成重复执行。
>
> 队列模式下不要用 `recover` 处理 RUNNING 记录（接管机制已覆盖）。`dispatch` 只在启动时加载调度规则，
> 新增规则或 pause/resume 后需要重启。`serve` 仍可用，是单进程一体化模式。
>
> Windows 下只有 `Ctrl+C` 能触发优雅退出。

### 可靠性语义

- **至少一次投递**：Worker 落库成功后才 ack；落库失败或 Worker 崩溃，消息留在 pending，租约过期后被其他 Worker 接管。
- **重复消费保护**：同一个 `(run_id, attempt)` 已处理过、或幂等键已属于别的 run 的重复消息，ack 后跳过，不会再执行。
- **重试**：失败且有次数时写入 `next_retry_at`，由 `dispatch` 进程里的 sweeper 到期后投递，沿用同一个 `run_id`。
- 设计与取舍见 `docs/adr/0005-queue-and-worker-design.md`。

## 可观测性（阶段 9）

### 结构化日志

    $env:TASK_PLATFORM_LOG_FORMAT = "json"     # text（默认）| json
    $env:TASK_PLATFORM_LOG_LEVEL = "INFO"      # 默认 INFO

JSON 日志每行一条，包含 `timestamp`、`level`、`service`、`logger`、`message`，
以及 `job_id`、`run_id`、`attempt` 等上下文字段。任务失败后可以用 `run_id` 过滤出完整执行链路。
`service` 取值：scheduler / dispatcher / worker / alerts / api / cli。
键名含 password / token / secret 等的字段会被脱敏为 `***`（只检查字段名，不要把敏感值写进 message）。

### 指标

    Invoke-RestMethod http://127.0.0.1:8000/api/v1/metrics
    Invoke-RestMethod "http://127.0.0.1:8000/api/v1/metrics?window_seconds=3600"
    (Invoke-WebRequest http://127.0.0.1:8000/metrics).Content

指标从数据库（执行记录）现算：各状态次数、成功率/失败率、平均与最大耗时、重试次数。
成功率 = SUCCESS / (SUCCESS + FAILED)，还没有终态时为 `null`。
队列长度默认不采集，启动 API 前设置 `TASK_PLATFORM_METRICS_QUEUE=1`（需要可连接的 Redis）。

Prometheus 指标：`job_execution_total{job,status}`、`job_executions_active{job,status}`、
`job_retry_total{job}`、`job_duration_seconds_sum/_count{job}`、`job_duration_seconds_max{job}`、
`queue_depth{queue}`、`queue_pending{queue}`。

抓取配置（**必须** `honor_labels: true`，否则应用的 `job` 标签会被改名为 `exported_job`）：

    scrape_configs:
      - job_name: task-platform
        metrics_path: /metrics
        honor_labels: true
        static_configs:
          - targets: ["localhost:8000"]

PromQL 示例：

    # 最近 1 小时成功率
    sum by (job) (increase(job_execution_total{status="SUCCESS"}[1h]))
      / sum by (job) (increase(job_execution_total{status=~"SUCCESS|FAILED"}[1h]))
    # 平均耗时
    rate(job_duration_seconds_sum[5m]) / rate(job_duration_seconds_count[5m])

### 告警

    python -m task_platform.cli.main alerts                 # 评估一次：无告警退出码 0，有告警退出码 1
    python -m task_platform.cli.main alerts --with-queue    # 同时检查队列积压（需要 Redis）
    python -m task_platform.cli.main alerts --watch 10      # 常驻，每 10 秒检查一次，Ctrl+C 退出

`GET /api/v1/alerts` 返回当前正在触发的告警。常驻模式下告警只在"新触发 / 已恢复"时写一次日志。

| 规则 | 触发条件 | 默认 | 环境变量 |
| --- | --- | --- | --- |
| consecutive_failures（CRITICAL） | 最近的最终结果连续 N 次 FAILED | 3 | `TASK_PLATFORM_ALERT_CONSECUTIVE_FAILURES` |
| timeouts（WARNING） | 窗口内超时尝试数 >= N | 3 次 / 3600 秒 | `TASK_PLATFORM_ALERT_TIMEOUT_COUNT`、`TASK_PLATFORM_ALERT_TIMEOUT_WINDOW_SECONDS` |
| queue_backlog（WARNING） | 队列未消费消息数 >= N | 100 | `TASK_PLATFORM_ALERT_QUEUE_DEPTH` |

已停用的任务不告警；队列读不到时不产生告警。设计与取舍见 `docs/adr/0006-observability-design.md`。

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
| GET | /api/v1/metrics | 任务指标快照（JSON），支持 `window_seconds` |
| GET | /metrics | Prometheus 文本格式指标（全量，不带窗口） |
| GET | /api/v1/alerts | 当前正在触发的告警（实时评估） |

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

当前阶段：**阶段 9 - 可观测性**（已完成）

阶段 9 交付：
- 结构化 JSON 日志（`structured_logging`、`JsonFormatter`），带 `job_id` / `run_id`，敏感字段脱敏
- 指标采集 `collect_metrics`：状态计数、成功/失败率、耗时、重试次数、队列长度
- 指标接口：`GET /api/v1/metrics`（JSON）、`GET /metrics`（Prometheus 文本）
- 基础告警：连续失败 / 超时 / 队列积压；`alerts` 命令（含 `--watch`）、`GET /api/v1/alerts`、告警日志
- 设计记录 `docs/adr/0006-observability-design.md`

DoD 对照（蓝图 8.10 / 19 节）：

| 项 | 状态 |
| --- | --- |
| 代码：结构化日志 / 指标 / 告警 | 完成 |
| 测试：告警规则测试 | 完成（`test_alerts`、`test_alerts_rules`、`test_alerts_cli_api`、`test_metrics`、`test_api_metrics`、`test_prometheus`、`test_structured_logging`） |
| 文档：仪表盘截图说明 | 部分完成：提供 Prometheus 抓取配置与 PromQL 示例；Grafana 仪表盘属可选项，未做 |
| 可运行：可观测 | 完成（不进入代码，通过日志、`/metrics`、`/api/v1/alerts`、`alerts` 命令判断健康状态） |

已知限制见 ADR-0006（未实现 scheduler_delay / data_quality 指标、告警无历史与通知渠道、超时按消息前缀识别）。

下一阶段：阶段 10 - Docker 与 CI（Dockerfile、Compose、PostgreSQL、CI 流水线）。
