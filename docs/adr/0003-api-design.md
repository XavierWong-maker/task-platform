# ADR-0003：阶段 6 API 设计

## 背景
阶段 6 需要把阶段 1–5 的能力通过 HTTP 暴露出来：任务管理、手动触发、执行历史与日志查询。
验收标准是 API 与 CLI 对同一任务得到一致的业务结果，并自动生成 OpenAPI 文档。

## 决策
1. **应用工厂 + lifespan**：`create_app(database_url=None)` 返回 FastAPI 实例；
   迁移、引擎、registry、线程池、日志收集器都在 lifespan 里创建，放入 `app.state`，
   退出时统一关闭。测试可以为每个用例注入独立数据库。
2. **API 层不直接碰数据库**：路由只通过 Repository 读写；每个请求一个事务，
   由 `get_session` 依赖复用 `session_scope`（成功提交，异常回滚）。
3. **请求/响应模型与领域层、ORM 分离**：`api/schemas.py` 独立定义；
   响应用 `from_attributes` 从领域对象转换，不暴露数据库细节。
4. **统一错误格式**：`{"error": {"code", "message", "details"}}`；
   业务错误、422 校验错误、框架 404/405/503、未捕获 500 全部走同一处理器，
   500 只记日志，不向客户端泄露内部信息。
5. **写操作的约束**：创建时 handler 必须已在 TaskRegistry 注册（白名单，不执行任意输入）；
   重名返回 409（先查重，再用 IntegrityError 兜底并发）；
   PATCH 用 `extra="forbid"` + `exclude_unset`，`handler` / `job_type` 不可修改，
   `retry_policy` 整体替换。
6. **手动触发同步执行一次尝试**：复用 `execute_attempt`，与 CLI `run` 语义一致；
   失败且还有重试额度时停在 RETRYING，重试仍由 Scheduler 负责。
   暂停的任务仍可手动运行（pause 只影响调度）。
7. **进程级共享资源**：LogCollector 与 ThreadPoolExecutor 随应用生命周期创建。
   不在每个请求里使用 `collecting_logs`，因为它会修改全局 logger 级别，并发请求会互相干扰。
8. **查询参数在入口校验**：分页 `page>=1`、`1<=page_size<=100`（日志上限 1000）；
   时间过滤必须带时区，否则 422（naive datetime 传到 `UTCDateTime` 会变成 500）。
9. **运行详情以 `run_id` 为键**：一次运行的多次尝试按 attempt 升序返回，
   整体 status 取最后一次尝试的状态；run 不存在统一 404 `run_not_found`。
10. **Pydantic 默认值写成 `Field(default=...)`**：mypy 靠关键字 `default=` 识别字段默认值，
    位置参数会被当成必填字段。

## 替代方案
- 路由里直接操作 ORM：被否决，会绕过 Repository，破坏分层和事务约定。
- 手动触发后台异步执行并立即返回：本阶段被否决，需要取消/恢复/队列配合，留到阶段 7/8。
- 把 ORM / 领域对象直接作为响应模型：被否决，字段变更会直接改变 API 契约。
- 每个请求挂一个 LogCollector：被否决，见决策 7。

## 影响
- API 响应字段是对外契约；修改 schema 需要考虑兼容性，破坏性变更走新版本前缀。
- 新增资源时：加 schema → 加 router → 在 `create_app` 里 include → 补集成测试。

## 已知限制
- 手动触发同步占用请求线程，最长为任务 timeout；真正的后台执行留到阶段 7/8。
- pause/resume 只修改数据库；已在运行的 `serve` 进程持有内存中的旧 `enabled`，
  需重启生效，阶段 8（Scheduler 与 Worker 解耦）解决。
- 通过 `@job` 注册的任务，执行时 timeout/retry 以 handler 元数据为准，
  API 修改这两项对这类任务暂不生效；内置任务的这两项每次启动还会被
  `sync_jobs_to_db` 用代码值覆盖（见 ADR-0001 决策 3）。
- 暂无认证与权限，API 仅用于本地开发；JWT/RBAC 属于进阶阶段。
