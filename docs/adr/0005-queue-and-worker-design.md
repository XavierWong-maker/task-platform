# ADR-0005：阶段 8 队列与 Worker 解耦设计

## 背景
阶段 7 之前，Scheduler 在同一个进程里"到期即执行"：一个长任务会阻塞同一轮的其他任务（ADR-0004 已知限制），
进程崩溃后只能靠人工 `recover`。阶段 8 要把"什么时候执行"和"怎么执行"拆到不同进程，
并回答：Worker 崩溃了任务怎么办？消息重复投递会不会重复执行？重试排期怎么跨进程保存？
验收标准（蓝图 8.9）：启动多个 Worker 后任务可被分发执行，单个 Worker 故障不会导致任务丢失。

## 决策
1. **队列用 Redis Streams + 消费者组**：消费后进入 pending，ack 前一直保留，
   空闲超时的 pending 可被他人接管（XAUTOCLAIM），天然支持"至少一次"。
2. **消息只带引用**（`job_id / run_id / attempt / idempotency_key`），不带任务定义；
   定义以数据库为准，Worker 崩溃不会丢失定义。反序列化忽略未知字段，便于以后加字段。
3. **`TaskQueue` 协议 + 两个实现**：`RedisStreamQueue` 用于生产，`MemoryQueue` 模拟同样的消费者组语义，
   用于单元测试和不依赖 Redis 的集成测试（时钟可注入，租约超时用假时钟验证）。
4. **调度与执行分离**：新增 `Dispatcher`（只认领幂等键并投递）与 `Worker`（只消费并执行）。
   旧的 `Scheduler` / `serve` 保留，作为单进程一体化模式，现有测试与用法不变。
5. **Dispatcher 的顺序**：认领幂等键 -> 投递 -> 尽力落库 QUEUED 行。消息是"待办"的事实来源，
   QUEUED 行只是给历史/API 看的投影，落库失败只记日志（Worker 用 upsert，不依赖它）。
   投递失败：释放幂等键，触发项以原槽位放回堆里，下一轮重试，不丢当天触发。
6. **重试排期持久化**：失败且还有次数的尝试写入 `job_executions.next_retry_at`（迁移 0004）；
   `sweep_due_retries` 先用 `UPDATE ... WHERE next_retry_at IS NOT NULL` 原子认领，再投递 `attempt+1`，
   多个 sweeper 并发只有一个会赢；投递失败则把排期写回。认领后、投递前崩溃留下的
   "next_retry_at 为空的 RETRYING"仍由 `recover` + `resume` 处理；
   `list_dangling_retries` 排除仍有排期的行。
7. **先落库、后 ack**：执行结果与日志在同一事务内 upsert，成功后才 ack；落库失败则消息留在 pending。
   `ack = XACK + XDEL`，否则已处理条目会一直留在 stream 里，`depth = XLEN - pending` 才精确。
8. **重复消费保护**（至少一次投递的代价）：取到消息先查 `(run_id, attempt)`，
   已是 SUCCESS / FAILED / CANCELED / RETRYING / RECOVERABLE 则 ack 后跳过；
   幂等键已被另一个 run 认领（Dispatcher 投递"成功但客户端超时"后补投留下的旧消息）也跳过。
   QUEUED / RUNNING / 无记录才执行；接管到 RUNNING 说明上一个持有者中途崩溃，重新执行。
9. **租约 = pending 空闲时间**：处理期间每隔 heartbeat 续约（`XCLAIM ... JUSTID`，重置空闲计时、
   不增加投递次数）；空闲超过 `stale_after` 的消息被其他 Worker 接管。
   接管按 `reclaim_interval` 节流，每次只领一条，处理完再领，避免一个 Worker 囤积多条消息而让它们的租约过期。
10. **毒消息**（无法解析）：记日志、ack 并删除，继续取下一条，避免 Worker 卡死在同一条消息上。
11. **Worker 内串行、进程间并行**：一个 Worker 一次处理一条消息；并行靠启动多个 Worker 进程。
12. **循环容错**：`run_loop` 捕获单步异常（如 Redis 暂时不可用），退避后继续，不让进程退出；
    信号只置位停止事件，当前消息处理完才退出（优雅关闭）。

## 替代方案
- Redis List（BRPOPLPUSH 等）：被否决，要自己实现 pending 列表、空闲检测和接管。
- Celery：被否决，本阶段目标是理解队列、ack、重投递与租约的原理；是否引入留作后续对比。
- 先 ack 后执行（至多一次）：被否决，Worker 崩溃会直接丢任务。
- 精确一次：分布式下无法保证，改为"至少一次 + 状态检查 + 幂等键"。
- 数据库轮询当队列：被否决，无法体现解耦，也难以扩展多 Worker。

## 影响
- 队列模式下**不要用 `recover` 处理 RUNNING 记录**：接管机制已覆盖，`recover` 会把仍在运行的记录误标为 RECOVERABLE。
  `recover` 仍用于 `serve` 模式，以及"认领后投递前崩溃"留下的悬空重试和孤儿幂等键。
- **所有 Worker 必须使用相同的 `--stale-after`**，并且每个 Worker 的心跳间隔都要小于最小的 `stale-after`；
  否则配置较短的 Worker 会抢走配置较长的 Worker 仍在执行的消息，造成重复执行。
- 新增 Redis 依赖；`TASK_PLATFORM_REDIS_URL`、`TASK_PLATFORM_QUEUE_STREAM` 控制连接与 stream 名。
- 修改消息字段要保持向后兼容；新增状态或排期字段要同步迁移与恢复逻辑。

## 对 ADR-0004 已知限制的处理
| 限制 | 状态 |
| --- | --- |
| `serve` 的 tick 串行，长任务阻塞同轮其他任务 | 已解决（队列模式下执行不在调度进程里） |
| 无心跳/租约，`recover` 需人工确认 | 已解决（队列模式用租约 + 接管；`serve` 模式仍需人工） |
| 重试排期只在内存 | 已解决（`next_retry_at` 持久化 + sweeper） |
| 超时/取消后不配合的 handler 线程仍占线程池名额 | 未解决（Python 无法强杀线程） |
| RETRYING / QUEUED 无法取消 | 未解决 |
| `FlakyState` 在进程内存中 | 未解决（仅影响故障注入演示） |

## 已知限制
- **僵尸 Worker**：持有者因长时间停顿丢了租约却仍在运行，两边可能重复执行，最后写入者胜出；任务应尽量幂等。
- **Redis 丢数据**：开启 AOF（`--appendonly yes`）后最坏丢约 1 秒的写入；丢失的消息对应的 QUEUED 行不会自动重发。
- **无死信队列**：毒消息被直接丢弃；超过投递次数上限的消息也没有隔离处理。
- **消费者名残留**：默认消费者名含 pid，每次重启都会在消费者组里留下一个旧名字，需要时手动 `XGROUP DELCONSUMER` 清理。
- **Dispatcher 只在启动时加载调度规则**：新增规则、pause/resume 都要重启 `dispatch` 才生效（ADR-0003 的限制没有消除）。
- **多进程共用 SQLite** 写入会互相等待（默认 5 秒超时）；负载大时应切换 PostgreSQL（阶段 10）。
- **QUEUED 状态不可取消**：`request_cancel` 只处理 RUNNING 与 RECOVERABLE。
