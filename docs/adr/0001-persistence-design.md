# ADR-0001：阶段 4 持久化设计

## 背景
阶段 1–3 的 Job / Schedule / Execution 全部在内存里，进程退出即丢失。阶段 4 需要
"重启后任务和执行历史仍在"，并为阶段 6 的 API 做好查询（分页/过滤）准备。

## 决策
1. **领域对象与 ORM 分离**：domain 保持纯 dataclass，ORM 用 `*Record`，映射集中在
   `mappers.py`。领域层不依赖 SQLAlchemy。
2. **Repository 不提交事务**：写操作只 `flush()`，让约束冲突在调用点暴露；事务边界
   统一由调用方的 `session_scope` 控制，跨多个 Repository 的写入原子提交/回滚。
3. **按 name 对账（sync_jobs_to_db）**：`build_jobs()` 每次启动都生成新 id，直接入库
   会让 Job 每次重启变成新行。对账时保留 id/enabled/created_at（运行时状态），
   handler/timeout/retry 以代码为准。
4. **一次"尝试"一行 execution**，`(run_id, attempt)` 唯一约束在数据库层阻止重复；
   日志通过 `(run_id, attempt)` 复合外键归属，级联删除。
5. **Scheduler 不耦合数据库**：通过 `run_forever(on_tick=...)` 回调把结果交给 CLI
   持久化。日志用 `LogCollector`（logging.Handler）旁路收集，Executor 不改动。
6. **执行历史按 `id DESC` 排序**，不用 `started_at`：后者在未开始时为 NULL，
   各数据库的 NULL 排序规则不一致。
7. **Alembic 手写首个迁移**，并用 `compare_metadata` 测试防止与 ORM 模型漂移；
   迁移脚本放在包内，程序化调用不依赖工作目录。

## 替代方案
- 直接在 ORM 对象上做领域逻辑：被否决，会把持久化细节带进状态机/调度器。
- Repository 内自动 commit：被否决，无法表达"Job+Schedule 要么全成功要么全回滚"。
- 每次启动 `create_all`：被否决，无法演进表结构（已存在的库不会补新表）。

## 影响
- 修改表结构必须新增迁移（`alembic revision --autogenerate`），并保证
  `test_migrated_schema_matches_orm_models` 通过。
- 切换 PostgreSQL 时 Repository/Service 不需要改，主要是 engine 配置和迁移验证。
