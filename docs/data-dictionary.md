# 数据字典（阶段 5）

所有数据均由 `TestDataFactory` 生成（固定 seed，可复现）。

## 原始记录（etl/records.py）

### RawOrder
| 字段 | 类型 | 说明 |
| --- | --- | --- |
| order_id | str \| None | 订单号，形如 O00000001；脏数据可能缺失/重复 |
| user_id | str | 用户 ID，形如 U000001 |
| product_id | str | 商品 ID，形如 P00001 |
| quantity | int | 数量，合法值 > 0 |
| unit_price | Decimal | 单价，合法值 >= 0 |
| discount | Decimal | 折扣率，取值 0.00 / 0.05 / 0.10 / 0.20 |
| status | str | 合法值 PAID / CANCELED / PENDING |
| created_at | datetime \| None | 下单时间（UTC），不得晚于参考时间 |

### RawUser
user_id、region（NORTH/SOUTH/EAST/WEST/CENTRAL）、register_date、member_level（BRONZE/SILVER/GOLD/PLATINUM）

### RawProduct
product_id、category、price、cost（成本 <= 价格）、supplier

## 质量规则（etl/quality.py）

| 规则名 | 检查内容 | 级别 | 对应脏数据 |
| --- | --- | --- | --- |
| not_null:order_id | 订单号非空 | ERROR | MISSING_ID |
| not_null:created_at | 下单时间非空 | ERROR | MISSING_TIME |
| unique:order_id | 订单号唯一 | ERROR | DUPLICATE_ID |
| range:quantity | quantity > 0 | ERROR | BAD_QUANTITY |
| range:unit_price | unit_price >= 0 | ERROR | NEGATIVE_PRICE |
| enum:status | 状态在合法集合内（区分大小写） | ERROR | BAD_STATUS |
| reference:user_id | 用户必须存在于用户表 | ERROR | ORPHAN_USER |
| not_future:created_at | 下单时间 <= 参考时间 | WARNING | FUTURE_TIME |

ERROR 违规的记录进入 quarantine；WARNING 只计入质量报告，记录继续处理。

## 输出文件（`data/output/<run_id>/`）

### daily_report.csv
order_date、region、order_count（所有通过校验的订单）、paid_count（PAID 订单数）、gmv（PAID 订单金额合计）、avg_order_value（客单价 = gmv / paid_count，无 PAID 订单时为 0.00）

金额口径：`amount = quantity × unit_price × (1 − discount)`，按分四舍五入（ROUND_HALF_UP）。

### quality_report.json
config（seed / order_count / dirty_ratio）、total_rows、quarantined_rows、warning_rows、results（每条规则的 rule / passed / invalid_count）

### quarantine.jsonl
每行一条被隔离的订单：`{"order": {...原始字段...}, "violations": [{"rule", "severity", "message"}]}`
