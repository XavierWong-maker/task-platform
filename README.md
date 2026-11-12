# 任务调度与数据处理平台

任务调度与数据处理项目，所有数据均为模拟数据。

## 环境要求
- Python 3.12+
- conda

## 快速开始

    conda create -n task-platform python=3.12 -y
    conda activate task-platform
    pip install -e ".[dev]"
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

## DAG 工作流（阶段 11）

多个任务按依赖关系组成工作流：上游全部成功才运行；互不依赖的节点并行；某个节点失败时，它的下游被跳过，
不相关的分支继续运行。

### 定义与运行

定义用 JSON 文件登记（`job` 填 Job 名，可用 `list` 查看；`depends_on` 可省略）：

    python -m task_platform.cli.main workflow register configs/workflows/daily_orders_dag.json
    python -m task_platform.cli.main workflow list
    python -m task_platform.cli.main workflow run daily-orders-dag [--max-parallel 4]
    python -m task_platform.cli.main workflow history daily-orders-dag
    python -m task_platform.cli.main workflow show <workflow_run_id>

运行中按 `Ctrl+C` 会取消：未开始的节点直接取消，运行中的节点协作式取消。

### 状态与失败传播

| 节点状态 | 含义 |
| --- | --- |
| PENDING → RUNNING → SUCCESS / FAILED / CANCELED | 正常流转，终态不可再转换 |
| SKIPPED | 有上游处于 FAILED / SKIPPED / CANCELED，本节点不会运行 |

工作流状态由节点状态推导：全部成功 `SUCCESS`；有成功也有失败/跳过 `PARTIAL_FAILED`；没有任何成功 `FAILED`；有取消 `CANCELED`。
节点内的重试沿用 Job 自己的 `RetryPolicy`。

### 重试与重跑

    python -m task_platform.cli.main workflow retry <workflow_run_id>             # 沿用成功节点，重跑其余
    python -m task_platform.cli.main workflow rerun <workflow_run_id> <节点名>    # 重跑该节点及其全部下游

两者都会**新建一次运行**（`parent_run_id` 指向来源），来源运行保持不变；沿用的节点标记 `(reused)`，不会再执行。
重放使用来源运行当时的定义快照。`rerun` 要求该节点的上游都已成功，否则请先 `retry`。

### 示例：5 节点 ETL DAG

`extract → clean → validate → aggregate → report`，阶段之间用 `data/output/dag/` 里的文件交接
（产物说明见 `docs/data-dictionary.md`）。全部成功时输出与单体任务 `etl-daily_orders` 逐字节相同。

    python -m task_platform.cli.main workflow run daily-orders-dag

模拟某个节点失败（该节点每次尝试都失败），再修复并重试：

    $env:TASK_PLATFORM_ETL_DAG_FAIL_STAGE = "validate"    # extract|clean|validate|aggregate|report
    python -m task_platform.cli.main workflow run daily-orders-dag      # validate 失败，aggregate / report 被跳过
    Remove-Item Env:TASK_PLATFORM_ETL_DAG_FAIL_STAGE
    python -m task_platform.cli.main workflow retry <workflow_run_id>   # extract / clean 沿用，其余重新执行

数据量等参数沿用 ETL 的环境变量（`TASK_PLATFORM_ETL_ORDER_COUNT` 等）。同一个输出目录同时只应有一次运行。

### API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| PUT | /api/v1/workflows/{name} | 创建或整体替换定义，请求体 `{"nodes": [{"name", "job", "depends_on"}]}` |
| GET | /api/v1/workflows、/api/v1/workflows/{name} | 定义列表 / 详情 |
| DELETE | /api/v1/workflows/{name} | 删除定义及其全部运行历史 |
| POST | /api/v1/workflows/{name}/run | 运行（同步执行，返回各节点状态） |
| GET | /api/v1/workflow-runs | 运行历史，支持 `workflow`、`status`、分页 |
| GET | /api/v1/workflow-runs/{run_id} | 一次运行的各节点状态 |
| POST | /api/v1/workflow-runs/{run_id}/retry | 重试 |
| POST | /api/v1/workflow-runs/{run_id}/rerun | 重跑节点，请求体 `{"node": "..."}` |

重放不可行时返回 409（`run_not_replayable`），定义不合法或引用了不存在的 Job 返回 422（`workflow_invalid`）。
设计与已知限制见 `docs/adr/0008-workflow-dag-design.md`（例如：引擎进程崩溃后没有自动恢复、不经过队列 / Worker）。

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

## 容器化与 CI（阶段 10）

### PostgreSQL

驱动是 psycopg 3。`postgresql://` 和 `postgres://` 开头的 URL 会被自动改写为 `postgresql+psycopg://`：

    $env:TASK_PLATFORM_DB_URL = "postgresql://user:password@localhost:5432/task_platform"

PG 集成测试默认跳过。设置专用测试库后才会运行（**库名必须包含 `test`**，每个用例会清空 `public` schema）：

    docker run -d --name task-platform-pg -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=task_platform_test -p 15432:5432 postgres:16-alpine
    $env:TASK_PLATFORM_TEST_PG_URL = "postgresql+psycopg://postgres:postgres@localhost:15432/task_platform_test"
    python -m pytest tests/integration/test_postgres.py

### Docker Compose：一条命令启动完整平台

    docker compose up -d --build
    docker compose ps -a

| 服务 | 作用 |
| --- | --- |
| postgres / redis | 数据库与队列，带健康检查；不向宿主机发布端口 |
| init | 一次性：执行迁移并同步任务定义；其余服务等它成功后才启动 |
| api | FastAPI，`http://127.0.0.1:8000`（`/docs`、`/health`、`/metrics`） |
| dispatch | 按调度规则向队列投递任务；没有调度规则时退出码 1，`on-failure` 会自动重启 |
| worker | 消费并执行任务；`stop_grace_period: 60s`，处理完当前消息才退出 |
| cli | 运维工具，不随 `up` 启动：`docker compose run --rm cli <子命令>` |

首次使用：

    docker compose run --rm cli schedule demo-success --interval 5
    docker compose restart dispatch              # dispatch 只在启动时加载调度规则
    docker compose up -d --scale worker=3        # 水平扩 Worker

- 所有服务默认输出 JSON 日志；api 开启了队列指标（`TASK_PLATFORM_METRICS_QUEUE=1`）。
- 数据库密码用 `POSTGRES_PASSWORD` 覆盖（默认 `postgres` 仅用于本地开发；可写在 `.env`，已被 `.gitignore` 忽略）。
- 数据保存在命名卷 `pg-data` / `redis-data` / `worker-data`。`docker compose down` 保留数据，**`down -v` 会删除**。
- 只有 worker 挂载了 `/app/data`：ETL 输出等文件在 api 或 `cli run` 的容器里是临时的。
- 镜像是单镜像多角色：默认命令是 API，dispatch / worker 在 Compose 里覆盖 `command`；以非 root 用户运行。

### CI（GitHub Actions）

`.github/workflows/ci.yml` 在 push 到 main/master 和每个 PR 上运行：

| job | 内容 |
| --- | --- |
| lint | `ruff check`、`ruff format --check`、`mypy src` |
| test | pytest，连真实的 PostgreSQL 与 Redis 服务容器；**任何测试被 skip 都算失败**（防止服务没起来时假绿） |
| smoke | `scripts/smoke_test.sh`：构建镜像、启动 Compose 栈，验证 dispatch → worker 端到端、`/metrics`、`/api/v1/alerts` 和 worker 的优雅退出 |

smoke 使用独立的 Compose 项目名 `task-platform-smoke`，不会影响本地开发栈的数据卷；本地运行需要先停掉占用 8000 端口的服务。设计取舍与已知限制见 `docs/adr/0007-docker-and-ci-design.md`。

## 项目状态

当前阶段：**阶段 11 - DAG 工作流**（已完成）。蓝图规划的 0–11 阶段全部完成。

阶段 11 交付：
- 工作流图结构：TaskNode / Dependency、环检测、拓扑排序与分层
- 节点 / 工作流状态机、失败传播规则
- 并行引擎：复用 `execute_attempt`，节点内重试、取消、`max_parallel`
- 持久化：迁移 0005 / 0006、运行历史、逐次尝试落库
- 工作流级重试、单节点重跑（新建运行 + 沿用结果）
- CLI `workflow` 子命令与 REST 接口
- 5 节点 ETL DAG 及端到端验收测试
- 设计记录 `docs/adr/0008-workflow-dag-design.md`

DoD 对照（蓝图 19 节，阶段 11）：

| 项 | 状态 |
| --- | --- |
| 代码：DAG | 完成 |
| 测试：拓扑 / 失败传播测试 | 完成（`test_workflow_graph.py`、`test_workflow_state.py`、`test_workflow_engine.py`、`test_dag_acceptance.py`） |
| 文档：工作流示例 | 完成（`configs/workflows/daily_orders_dag.json`、本文 DAG 一章、ADR-0008） |
| 可运行：复杂 ETL | 完成（5 节点 ETL DAG） |

最终验收场景对照（蓝图 20 节）：

| # | 场景 | 对应能力 |
| --- | --- | --- |
| 1 | 每日订单 ETL，每天 01:00 | `schedule etl-daily_orders --daily 01:00`（按 UTC） |
| 2 | 调度器创建 execution，Worker 获取并运行 | `dispatch` + `worker`（阶段 8） |
| 3 | 10 万条订单，3% 脏数据 | ETL 默认配置 |
| 4 | 清洗、校验、转换、聚合，输出日报 | `etl-daily_orders` 与 DAG 版 ETL |
| 5 | 质量报告与 quarantine | `quality_report.json`、`quarantine.jsonl` |
| 6 | 随机失败一次，自动重试，最终成功 | `fault-flaky`、重试链路与 sweeper |
| 7 | API 查看执行记录、attempt、耗时、日志 | `/api/v1/executions*` |
| 8 | 停止 Worker 后恢复，不产生错误的重复写入 | 租约接管 + 幂等键 + 重复消费保护 |
| 9 | 两个 Worker 并行 | 多 Worker 消费（`test_multi_worker.py`） |
| 10 | Docker Compose 启动，CI 通过 | 阶段 10 |
| 11 | 5 节点 DAG，节点失败时下游行为符合预期 | `test_dag_acceptance.py` |

未做 / 已知限制：
- 蓝图 17 节的**资源限制**未做；没有镜像仓库推送和 CD；认证与权限（JWT / RBAC）未做。
- 工作流引擎崩溃后没有自动恢复，且不经过队列 / Worker；API 运行接口为同步执行（见 ADR-0008）。
- 依赖未固定版本，没有 lockfile（见 ADR-0007）。

各阶段的设计取舍见 `docs/adr/0001` ~ `0008`。
