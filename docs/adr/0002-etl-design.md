# ADR-0002：阶段 5 ETL 设计

## 背景
阶段 5 需要在平台上跑一个"每日订单 ETL"，处理 100 万量级的模拟数据，
并输出日报、质量报告和被隔离的坏数据。

## 决策
1. **模拟数据可复现**：每个 `generate_*` 调用都新建 `random.Random(f"{seed}:{域}")`，
   参考时间 `now` 由外部注入，不读真实时间。
2. **脏数据带标签**：`generate_labeled_orders` 同时返回注入的脏数据类型，
   质量规则的测试直接对照"标准答案"，而不是复述规则本身。
3. **流水线全程 generator**：validate / transform / aggregate 逐条流过；
   聚合状态只与"日期数 × 地区数"有关，与订单量无关。
4. **规则分 ERROR / WARNING**：ERROR 进 quarantine，WARNING（未来时间）只计数不隔离。
5. **quarantine 通过回调输出**：pipeline 不决定存放位置，调用方传入 `on_quarantine`；
   日报任务边处理边写 jsonl，不在内存累积。
6. **金额用 Decimal，舍入显式 ROUND_HALF_UP**：测试曾因默认的 ROUND_HALF_EVEN
   与业务口径不一致而失败，此后金额相关代码一律显式指定舍入方式。
7. **业务逻辑与任务入口分离**：`etl/daily_orders.py` 是普通函数（可直接测试），
   `jobs/etl_jobs.py` 只读配置并调用它。
8. **输出目录按 run_id 隔离**；真正的幂等（重复运行不产生重复写入）留到阶段 7。

## 替代方案
- 一次性 `list` 装入全部订单：被否决，内存随数据量线性增长（见规模验证）。
- 用 pandas 做聚合：被否决，阶段目标是掌握 generator 与流式处理原理。
- 在 pipeline 内直接写 quarantine 文件：被否决，会让流水线耦合文件系统，不便测试。

## 影响
- `UniqueRule` 需要记住全部 order_id，内存随数据量线性增长；
  超大数据量（千万级以上）需改为分区去重或数据库去重。
- 新增质量规则只需实现 `DataQualityRule` 并加入 `default_order_rules`。

## 规模验证结果（100 万条，Python 3.12，本机 Windows）
- streaming：耗时 10.54 s，吞吐约 94,900 rows/s
- streaming 内存峰值：79.7 MB（tracemalloc），主要来自 UniqueRule 记住的 order_id
- materialized 内存峰值：430.5 MB（tracemalloc），约为 streaming 的 5.4 倍
- 隔离 26,065 条，与理论值（3% × 7/8 ≈ 26,250）一致
- 结论：百万级无需优化 UniqueRule；千万级（约 800 MB）才需要分区或数据库去重
