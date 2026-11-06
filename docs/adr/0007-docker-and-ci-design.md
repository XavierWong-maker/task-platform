# ADR-0007：阶段 10 容器化、PostgreSQL 与 CI 设计

## 背景
阶段 9 之后系统由 API、dispatch、worker、数据库与 Redis 组成，只能靠人手分别启动。
阶段 10 要回答：新环境能否一条命令启动完整平台？SQLite 之外的 PostgreSQL 行为是否一致？
每次提交能否自动验证这一切？验收标准（蓝图 8.11）：一条命令启动完整环境，CI 完成测试与检查。

## 决策
1. **单镜像多角色**：同一个镜像，默认命令是 API，dispatch / worker 在 Compose 里覆盖 `command`。
   健康检查因此放在 Compose 里按服务配置，不写进 Dockerfile。
2. **依赖从 `pyproject.toml` 读出安装，代码用 `PYTHONPATH=/app/src`**，不执行 `pip install .`：
   非 editable 安装不会带上 `migrations/script.py.mako`，容器内 `upgrade_db()` 会找不到迁移脚本。
   依赖层与代码层分开，改代码不重装依赖。镜像以非 root 用户运行。
3. **PostgreSQL**：驱动只用 psycopg 3，`normalize_database_url` 把 `postgresql://`、`postgres://`
   统一改写为 `postgresql+psycopg://`；非 SQLite 启用 `pool_pre_ping`。表结构一律走 Alembic 迁移。
4. **PG 测试是可选加入的**：设置 `TASK_PLATFORM_TEST_PG_URL` 才运行；库名必须含 `test`（每个用例会清空 schema）。
   覆盖 SQLite 测不出的部分：迁移与 ORM 无漂移、downgrade、复合外键级联、真并发认领、指标与告警 SQL。
5. **幂等键认领改用 `RETURNING` 判断输赢**：PG 测试发现 `try_claim` 在并发下 8 个线程全部返回 False
   （没有赢家），原先依赖 `rowcount == 1`。改为 `INSERT ... ON CONFLICT DO NOTHING RETURNING key`，
   返回行即认领成功。教训：此前有用例调用了 `try_claim` 却没断言返回值，空转通过；
   新增了断言和单线程用例。
6. **`init` 一次性服务**：执行迁移并同步任务定义，其余服务用 `service_completed_successfully` 等待。
   否则多个服务首次同时启动，会在 `jobs.name` 唯一约束上互相冲突。
7. **Compose 约定**：PG / Redis 不向宿主机发布端口；全部服务 `LOG_FORMAT=json`，api 开启队列指标
   （ADR-0006 的约定）；worker `stop_grace_period: 60s`；dispatch `restart: on-failure`
   （没有调度规则时退出码 1）；运维命令放在 `tools` profile 的 `cli` 服务。
8. **CI 三个 job**：`lint`、`test`、`smoke`（依赖前两者）。`test` 用服务容器连真实 PG 与 Redis，
   并在日志出现 `SKIPPED` 时失败——服务没起来时 pytest 仍返回 0，会假绿。
9. **smoke 脚本的设计**：独立项目名，`down -v` 不碰本地数据；两段启动（先 api，加调度规则，
   再起 dispatch 与 worker），避免 dispatch 因没有规则反复重启；断言业务结果
   （至少 2 条 SUCCESS、`/metrics` 有 `queue_depth`、`/alerts` 可用）；
   `docker compose stop worker` 后退出码必须为 0，验证容器内优雅退出；失败时输出各服务日志。
10. **runner 与 action 版本**：runner 固定为 `ubuntu-24.04`；`checkout`、`setup-python` 升到 v6（Node 24）。
11. **类型收窄**：CI 的 mypy 报 `list_durations` 里 `datetime | None` 相减；该处 SQL 已过滤空值，
    补上 `is not None` 判断让类型成立。本地没报错，推测与依赖版本差异有关（未核实）。

## 替代方案
- `pip install .` 打包安装：被否决，见决策 2。
- `docker compose up --wait` 做健康等待：被否决，一次性的 `init` 服务会退出，自写等待脚本更可控。
- 各服务启动时自己跑迁移：被否决，首次并发启动会竞争；统一由 `init` 完成。
- 在 CI 里直接 `docker compose up` 不写脚本：被否决，无法断言业务结果与优雅退出。
- 引入 Celery / Kubernetes：超出本阶段范围。

## 影响
- 修改 `ci.yml` 的 job 名要同步 GitHub 分支保护规则（如果以后开启）。
- 新增服务要同时考虑：是否需要健康检查、是否依赖 `init`、是否要加进 smoke 脚本。
- 新增 PG 相关行为先写进 `test_postgres.py`，再改 Repository。

## 已知限制
- **依赖未固定版本，没有 lockfile**：CI 每次装最新版，可能出现"本地绿、CI 红"（决策 11 就是一例）。
  后续可加 constraints 文件或 lockfile。
- smoke 只覆盖 `demo-success`；ETL、重试、取消在 Compose 下没有端到端覆盖（它们由 pytest 覆盖）。
- 只有 worker 挂载 `/app/data`；api 与 `cli run` 产生的文件在容器里是临时的。
- 没有资源限制（蓝图 17 节的部署前项之一）、没有镜像仓库推送和 CD、没有密钥管理（只有环境变量）。
- `dispatch` 只在启动时加载调度规则，新增规则后需 `docker compose restart dispatch`（ADR-0005 的限制）。
- 仍无认证与权限（JWT / RBAC 属进阶阶段）。
