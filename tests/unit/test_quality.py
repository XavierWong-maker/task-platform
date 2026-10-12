from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import pytest

from task_platform.etl.factory import DEFAULT_NOW, TestDataFactory
from task_platform.etl.quality import (
    EnumRule,
    NotFutureRule,
    NotNullRule,
    QualityChecker,
    RangeRule,
    ReferenceRule,
    Severity,
    UniqueRule,
    default_order_rules,
    is_quarantined,
)
from task_platform.etl.records import DirtyKind, RawOrder

_EXPECTED_RULE = {
    DirtyKind.MISSING_ID: "not_null:order_id",
    DirtyKind.DUPLICATE_ID: "unique:order_id",
    DirtyKind.BAD_QUANTITY: "range:quantity",
    DirtyKind.NEGATIVE_PRICE: "range:unit_price",
    DirtyKind.BAD_STATUS: "enum:status",
    DirtyKind.ORPHAN_USER: "reference:user_id",
    DirtyKind.FUTURE_TIME: "not_future:created_at",
    DirtyKind.MISSING_TIME: "not_null:created_at",
}


def _order(**overrides: object) -> RawOrder:
    fields: dict[str, object] = {
        "order_id": "O1",
        "user_id": "U000001",
        "product_id": "P00001",
        "quantity": 1,
        "unit_price": Decimal("10.00"),
        "discount": Decimal("0.00"),
        "status": "PAID",
        "created_at": DEFAULT_NOW - timedelta(days=1),
    }
    fields.update(overrides)
    return RawOrder(**fields)  # type: ignore[arg-type]


# ---------------- 单条规则 ----------------


def test_not_null_rule() -> None:
    rule = NotNullRule("order_id")
    assert rule.check(_order()) is None
    assert rule.check(_order(order_id=None)) is not None


def test_unique_rule_is_stateful_and_ignores_none() -> None:
    rule = UniqueRule("order_id")
    assert rule.check(_order(order_id="A")) is None
    assert rule.check(_order(order_id="B")) is None
    assert rule.check(_order(order_id="A")) is not None
    assert rule.check(_order(order_id=None)) is None
    assert rule.check(_order(order_id=None)) is None


@pytest.mark.parametrize(("value", "ok"), [(1, True), (0, False), (-3, False)])
def test_range_rule_exclusive(value: int, ok: bool) -> None:
    rule = RangeRule("quantity", minimum=0, inclusive=False)
    assert (rule.check(_order(quantity=value)) is None) is ok


@pytest.mark.parametrize(
    ("value", "ok"),
    [(Decimal("0"), True), (Decimal("9.99"), True), (Decimal("-0.01"), False)],
)
def test_range_rule_inclusive(value: Decimal, ok: bool) -> None:
    rule = RangeRule("unit_price", minimum=Decimal("0"))
    assert (rule.check(_order(unit_price=value)) is None) is ok


def test_enum_rule_is_case_sensitive() -> None:
    rule = EnumRule("status", ("PAID", "CANCELED", "PENDING"))
    assert rule.check(_order(status="PAID")) is None
    assert rule.check(_order(status="paid")) is not None


def test_reference_rule() -> None:
    rule = ReferenceRule("user_id", ["U000001"])
    assert rule.check(_order()) is None
    assert rule.check(_order(user_id="U999999")) is not None


def test_not_future_rule_is_warning_and_boundary_is_ok() -> None:
    rule = NotFutureRule("created_at", DEFAULT_NOW)
    assert rule.severity is Severity.WARNING
    assert rule.check(_order(created_at=DEFAULT_NOW)) is None
    assert rule.check(_order(created_at=DEFAULT_NOW + timedelta(seconds=1))) is not None
    assert rule.check(_order(created_at=None)) is None


# ---------------- Checker ----------------


def test_checker_rejects_duplicate_rule_names() -> None:
    with pytest.raises(ValueError):
        QualityChecker([NotNullRule("order_id"), NotNullRule("order_id")])


def test_clean_row_has_no_violations() -> None:
    checker = QualityChecker(default_order_rules(["U000001"], DEFAULT_NOW))
    assert checker.check(_order()) == []
    report = checker.report()
    assert report.total_rows == 1
    assert report.quarantined_rows == 0
    assert all(r.passed for r in report.results)


def test_future_time_warns_but_is_not_quarantined() -> None:
    checker = QualityChecker(default_order_rules(["U000001"], DEFAULT_NOW))
    violations = checker.check(_order(created_at=DEFAULT_NOW + timedelta(days=1)))

    assert [v.severity for v in violations] == [Severity.WARNING]
    assert is_quarantined(violations) is False
    report = checker.report()
    assert report.quarantined_rows == 0
    assert report.warning_rows == 1


def test_checker_matches_factory_labels_exactly() -> None:
    """用 factory 的标签当标准答案：干净行零违规，脏行恰好触发对应的那条规则"""
    factory = TestDataFactory(20260817, user_pool_size=50, product_pool_size=20)
    user_ids = {u.user_id for u in factory.generate_users()}
    checker = QualityChecker(default_order_rules(user_ids, DEFAULT_NOW))

    dirty = 0
    for order, kind in factory.generate_labeled_orders(20_000, dirty_ratio=0.1):
        violations = checker.check(order)
        if kind is None:
            assert violations == []
        else:
            dirty += 1
            assert [v.rule for v in violations] == [_EXPECTED_RULE[kind]]

    report = checker.report()
    assert report.total_rows == 20_000
    assert report.quarantined_rows + report.warning_rows == dirty
    assert sum(r.invalid_count for r in report.results) == dirty
    assert not any(r.passed for r in report.results)  # 每条规则都至少检出过一次
