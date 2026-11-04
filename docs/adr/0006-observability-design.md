# ADR-0006：阶段 9 可观测性设计

## 背景
阶段 8 之后系统由 API、dispatch、worker 等多个进程组成。阶段 9 要回答：
不进入代码，能否从日志和指标判断系统当前是否健康？任务失败后能否凭 job_id + run_id 串起整条链路？
验收标准（蓝图 8.10）：不进入代码也能从日志/指标判断系统当前是否健康。

## 决策
1. **结构化日志用标准库实现**：`JsonFormatter` 把 logging 的 `extra` 字段展开为 JSON 顶层键，
   executor / scheduler / worker 现有的日志调用不需要改。核心字段（timestamp / level / service /
   logger / message）不允许被 `extra` 覆盖。
2. **日志配置用上下文管理器 `structured_logging`**：挂在 `task_platform` logger 上，退出时卸载并还原级别，
   不使用 `basicConfig`（不改全局 root，测试之间互不污染）。
   环境变量 `TASK_PLATFORM_LOG_FORMAT`（默认 text，开发友好）、`TASK_PLATFORM_LOG_LEVEL`。
   `service` 字段按子命令区分：scheduler / dispatcher / worker / alerts / api / cli。
3. **敏感字段脱敏**：`extra` 的键名含 password / token / secret / authorization / api_key 时输出 `***`。
   只检查键名，不扫描 message 文本，所以不要把敏感值拼进 message。
4. **指标从数据库现算，不用进程内计数器**：API、dispatch、worker 是不同进程，
   只有数据库（执行记录）与队列是跨进程一致的数据源。`collect_metrics` 返回不可变快照。
5. **指标口径**：成功率 = SUCCESS / (SUCCESS + FAILED)，CANCELED / RETRYING 不计入，分母为 0 时为 None（不是 0）；
   耗时只统计 SUCCESS / FAILED；重试次数 = attempt > 1 的行数；可选时间窗口按 `started_at` 过滤
   （因此带窗口时尚未开始的 QUEUED 不计入）。
6. **队列指标可选**：队列未提供或读取失败时队列指标为空并记日志，不让整份快照失败。
   API 默认不连 Redis，设置 `TASK_PLATFORM_METRICS_QUEUE=1` 才连接（避免本地启动或测试误连）。
7. **Prometheus 文本自己渲染，不引入 `prometheus_client`**：指标是每次请求现算的快照，
   不是进程内累加的计数器。终态计数用 counter（`job_execution_total`），进行中的状态会下降，
   所以用 gauge（`job_executions_active`）。`/metrics` 总是全量，窗口计算交给 Prometheus 的 `rate()`。
8. **标签名 `job` 与 Prometheus 的内置标签冲突**：沿用蓝图 14.2 的 `job_execution_total{job,status}`。
   Prometheus 抓取时会再加一个 `job="<抓取任务名>"`，冲突时把应用的标签改名为 `exported_job`。
   抓取配置必须设置 `honor_labels: true`。
9. **告警是对当前状态的实时评估，不是事件流**：三条规则——连续失败（CRITICAL）、窗口内超时次数、队列积压。
   `consecutive_failures` 只看 SUCCESS / FAILED 的最终结果，RETRYING 不算，一次运行重试后失败只算一次；
   已停用的任务不告警；队列读不到时不产生告警（避免 Redis 故障被当成积压）。
10. **告警去重在 `AlertTracker`**：常驻进程只在"新触发 / 已恢复"时写日志，不每轮重复。
    告警通过三个出口发出：结构化日志（`alert_rule` 等字段）、`alerts` 子命令（有告警退出码 1，便于脚本/cron）、
    `GET /api/v1/alerts`。
11. **超时通过错误消息前缀识别**：数据库没有结构化的失败原因，所以把 executor 的超时消息前缀提成常量
    `TIMEOUT_ERROR_PREFIX`，告警按前缀匹配 `error_message`。

## 替代方案
- 进程内计数器 + `prometheus_client`：被否决，多进程下 worker 的计数 API 进程看不到，需要 pushgateway 或多进程模式。
- 把 JSON 日志格式设为默认：被否决，本地开发可读性差；容器部署（阶段 10）再通过环境变量打开。
- 给 execution 加"失败原因"结构化列：本阶段被否决（需要迁移与状态机配合），见已知限制。
- 告警事件持久化 + 通知渠道（蓝图的 Notification 实体）：本阶段被否决，范围过大。

## 影响
- 修改 executor 的超时错误消息时，必须同步 `TIMEOUT_ERROR_PREFIX`，否则超时告警失效。
- 新增指标先改 `metrics.py`，再改 `prometheus.py` 和 API schema；Prometheus 指标名与标签是对外契约。
- 阶段 10 的 Compose 里给各服务设置 `TASK_PLATFORM_LOG_FORMAT=json` 与 `TASK_PLATFORM_METRICS_QUEUE=1`。

## 已知限制
- 未实现 `scheduler_delay_seconds`（库里没存计划触发时刻）和 `data_quality_failure_total`
  （数据只在 ETL 输出的 `quality_report.json` 里）。
- 指标每次请求都查数据库：执行记录很多时耗时线性增长；耗时统计在 Python 里算，量大时应改为 SQL 聚合或预聚合。
  告警的连续失败规则对每个启用的任务各查一次（N+1）。
- 删除任务会级联删除其执行记录，该任务的 counter 随之归零（Prometheus 会当作 counter 重置）。
- 告警不保存历史，也没有通知渠道（邮件 / webhook）；只能靠日志、CLI、API 查看。
- `alerts --watch` 的状态在内存中，进程重启后会把仍在触发的告警当作"新触发"再报一次。
- 超时识别依赖消息前缀（见决策 11）。
- 没有仪表盘：Grafana 属于可选项，本阶段只提供 Prometheus 抓取端点。
