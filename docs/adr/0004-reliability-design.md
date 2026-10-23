# ADR-0004：阶段 7 可靠性与并发控制设计

## 背景
阶段 1–6 默认"进程一直活着、只有一个 Scheduler"。阶段 7 要回答：
多个 Scheduler 同时到期会不会重复执行？进程崩溃后状态如何？任务能不能被取消？
验收标准：用测试复现竞态/重复执行问题，并证明设计可以控制它。

## 决策
1. **幂等键靠数据库主键冲突认领**：`idempotency_keys(key PK, job_id, run_id)`，
   `INSERT ... ON CONFLICT DO NOTHING`，返回 rowcount 判断输赢。不用"先查后写"，
   也不用 SAVEPOINT（pysqlite 下不可靠）。
2. **键由"计划触发时刻"推导**，不是 now：多个 Scheduler 对同一槽位算出同一个键。
   DAILY 键 = job:日期:schedule_id；INTERVAL 键额外带槽位时刻。
   只有 attempt=1 的常规触发认领，重试沿用原 run_id、不再认领。手动 run 不认领。
3. **Scheduler 不依赖数据库**：通过注入的 `claim` / `on_start` 回调接入持久化。
   回调抛异常只跳过本次触发、排期照常推进。
4. **execution 在进入 RUNNING 时就落库**（`on_started` 回调），崩溃后库里有痕迹；
   结束时用 `upsert` 走更新分支。
5. **新增状态 RECOVERABLE**：RUNNING / RETRYING -> RECOVERABLE -> QUEUED/FAILED/CANCELED。
   状态列是 VARCHAR，无需迁移。
6. **恢复是显式命令 `recover`，不在 serve 启动时自动执行**：当前没有心跳/租约，
   自动恢复会把另一个健康 serve 的 RUNNING/RETRYING 误判为崩溃。
7. **协作式取消**：`CancelToken`（threading.Event）经 `JobContext.check_cancelled()` 暴露；
   取消后给 `cancel_grace_seconds` 宽限期，不配合则放弃等待并记 CANCELED。
   线程无法被强杀，这是 Python 的事实，不是实现缺陷。
8. **超时也触发令牌**：配合的 handler 能真正停下来。超时/取消判定改用
   `wait()` 轮询，避免与 handler 自己抛出的 TimeoutError 混淆。
9. **跨进程取消走数据库标志位 + watcher 线程**：`cancel_requested` 列由 `request_cancel` 写入，
   `update_execution_record` 不回写它，防止被执行结束的更新覆盖。
10. **悬空 RETRYING 走 recover + resume**：重试排期不持久化；崩溃后标记 RECOVERABLE，
    `resume` 沿用同一 run_id、attempt+1。
11. **故障注入用确定性计数而非随机数**：`FlakyState` 按 run_id 计数，测试不偶发失败。

## 恢复策略一览
| 崩溃时的状态 | recover 的处理 | 之后 |
| --- | --- | --- |
| RUNNING | -> RECOVERABLE（error 标记 interrupted） | resume / cancel-run |
| 悬空 RETRYING | -> RECOVERABLE（保留上次失败原因） | resume / cancel-run |
| 已认领但无 execution（孤儿键） | 释放键 | 槽位可被再次调度 |
| 已认领且有 execution | 键保留 | 防止恢复前重复写入 |

## 替代方案
- 先查后写去重：被否决，存在竞态（`test_race_conditions` 复现了无保护时的重复执行）。
- serve 启动时自动恢复：被否决，见决策 6；留给阶段 8 的租约/心跳。
- 持久化重试排期（next_retry_at）：本阶段被否决，阶段 8 引入队列后统一解决。
- 强杀线程/子进程隔离：被否决，超出本阶段范围；留给"隔离执行"扩展。

## 影响
- 修改执行路径要保持 `claim` / `on_start` 回调语义；新增状态需同步状态机测试。
- 取消是协作式的：长任务必须在循环中调用 `ctx.check_cancelled()`。

## 已知限制
- 超时/取消后不配合的 handler 线程仍占用线程池名额（`fault.hang` 可复现）。
- `serve` 的 tick 是串行的：一个长任务会阻塞同一轮其他任务（阶段 8 的 Worker 解耦解决）。
- RETRYING 无法跨进程取消；API 同步触发的运行无法取消。
- `FlakyState` 在进程内存中，重启后清零。
- 无心跳/租约：`recover` 需人工确认没有 serve 在运行。
