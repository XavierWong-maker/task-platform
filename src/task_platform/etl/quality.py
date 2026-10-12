"""数据质量规则与检查器

- 一条规则只判断一件事：违规返回描述字符串，通过返回 None
- ERROR 违规的记录应进入 quarantine；WARNING 只记录告警，不隔离
- UniqueRule 是有状态的（记住见过的值），因此规则实例应"每次运行新建一份"，
  可用 default_order_rules() 获得一套全新的规则
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import Counter
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from task_platform.etl.records import VALID_ORDER_STATUSES, RawOrder


class Severity(StrEnum):
    ERROR = "ERROR"  # 隔离
    WARNING = "WARNING"  # 仅告警


@dataclass(frozen=True, slots=True)
class Violation:
    rule: str
    severity: Severity
    message: str


class DataQualityRule(ABC):
    def __init__(self, name: str, severity: Severity = Severity.ERROR) -> None:
        self.name = name
        self.severity = severity

    @abstractmethod
    def check(self, order: RawOrder) -> str | None:
        """违规返回描述，通过返回 None"""


class NotNullRule(DataQualityRule):
    def __init__(self, field: str) -> None:
        super().__init__(f"not_null:{field}")
        self._field = field

    def check(self, order: RawOrder) -> str | None:
        if getattr(order, self._field) is None:
            return f"{self._field} is null"
        return None


class UniqueRule(DataQualityRule):
    """值重复则违规；None 不处理（那是 NotNullRule 的职责）"""

    def __init__(self, field: str) -> None:
        super().__init__(f"unique:{field}")
        self._field = field
        self._seen: set[Any] = set()

    def check(self, order: RawOrder) -> str | None:
        value = getattr(order, self._field)
        if value is None:
            return None
        if value in self._seen:
            return f"{self._field}={value!r} is duplicated"
        self._seen.add(value)
        return None


class RangeRule(DataQualityRule):
    """数值下界检查；inclusive=False 表示必须严格大于 minimum"""

    def __init__(self, field: str, *, minimum: int | Decimal, inclusive: bool = True) -> None:
        super().__init__(f"range:{field}")
        self._field = field
        self._minimum = minimum
        self._inclusive = inclusive

    def check(self, order: RawOrder) -> str | None:
        value = getattr(order, self._field)
        if value is None:
            return None
        ok = value >= self._minimum if self._inclusive else value > self._minimum
        if ok:
            return None
        op = ">=" if self._inclusive else ">"
        return f"{self._field}={value} violates {op} {self._minimum}"


class EnumRule(DataQualityRule):
    def __init__(self, field: str, allowed: Collection[str]) -> None:
        super().__init__(f"enum:{field}")
        self._field = field
        self._allowed = frozenset(allowed)

    def check(self, order: RawOrder) -> str | None:
        value = getattr(order, self._field)
        if value is None or value in self._allowed:
            return None
        return f"{self._field}={value!r} not in {sorted(self._allowed)}"


class ReferenceRule(DataQualityRule):
    """外键式关联检查：值必须存在于给定的合法集合中（孤儿记录）"""

    def __init__(self, field: str, valid_values: Collection[str]) -> None:
        super().__init__(f"reference:{field}")
        self._field = field
        self._valid = frozenset(valid_values)

    def check(self, order: RawOrder) -> str | None:
        value = getattr(order, self._field)
        if value is None or value in self._valid:
            return None
        return f"{self._field}={value!r} has no matching record"


class NotFutureRule(DataQualityRule):
    """时间不得晚于参考时间；只记警告，所以默认 WARNING"""

    def __init__(self, field: str, now: datetime) -> None:
        super().__init__(f"not_future:{field}", Severity.WARNING)
        self._field = field
        self._now = now

    def check(self, order: RawOrder) -> str | None:
        value = getattr(order, self._field)
        if value is None or value <= self._now:
            return None
        return f"{self._field}={value.isoformat()} is later than {self._now.isoformat()}"


def default_order_rules(user_ids: Collection[str], now: datetime) -> list[DataQualityRule]:
    """订单的标准规则集（每次调用返回全新实例，含有状态规则）"""
    return [
        NotNullRule("order_id"),
        NotNullRule("created_at"),
        UniqueRule("order_id"),
        RangeRule("quantity", minimum=0, inclusive=False),
        RangeRule("unit_price", minimum=Decimal("0")),
        EnumRule("status", VALID_ORDER_STATUSES),
        ReferenceRule("user_id", user_ids),
        NotFutureRule("created_at", now),
    ]


@dataclass(frozen=True, slots=True)
class RuleResult:
    """DataQualityResult：一条规则在本次运行中的结果"""

    rule: str
    passed: bool
    invalid_count: int


@dataclass(frozen=True, slots=True)
class QualityReport:
    total_rows: int
    quarantined_rows: int  # 至少有一条 ERROR 违规
    warning_rows: int  # 至少有一条 WARNING 违规
    results: list[RuleResult]


def is_quarantined(violations: Sequence[Violation]) -> bool:
    return any(v.severity is Severity.ERROR for v in violations)


class QualityChecker:
    """对记录逐条套用规则，并累计统计，最后产出 QualityReport"""

    def __init__(self, rules: Sequence[DataQualityRule]) -> None:
        names = [r.name for r in rules]
        if len(set(names)) != len(names):
            raise ValueError("rule names must be unique")
        self._rules = list(rules)
        self._failures: Counter[str] = Counter()
        self._total = 0
        self._quarantined = 0
        self._warned = 0

    def check(self, order: RawOrder) -> list[Violation]:
        violations: list[Violation] = []
        for rule in self._rules:
            message = rule.check(order)
            if message is not None:
                violations.append(Violation(rule.name, rule.severity, message))
                self._failures[rule.name] += 1

        self._total += 1
        if is_quarantined(violations):
            self._quarantined += 1
        if any(v.severity is Severity.WARNING for v in violations):
            self._warned += 1
        return violations

    def report(self) -> QualityReport:
        return QualityReport(
            total_rows=self._total,
            quarantined_rows=self._quarantined,
            warning_rows=self._warned,
            results=[
                RuleResult(r.name, self._failures[r.name] == 0, self._failures[r.name])
                for r in self._rules
            ],
        )
