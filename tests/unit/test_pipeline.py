from __future__ import annotations

from datetime import date, timedelta
from decimal import ROUND_HALF_UP, Decimal

from task_platform.etl.factory import DEFAULT_NOW, TestDataFactory
from task_platform.etl.pipeline import (
    CleanOrder,
    QuarantinedOrder,
    aggregate,
    run_orders_pipeline,
    transform,
    validate_stream,
)
from task_platform.etl.quality import QualityChecker, Severity, default_order_rules
from task_platform.etl.records import DirtyKind, RawOrder, RawUser

_USER = RawUser("U000001", "NORTH", date(2026, 1, 1), "GOLD")


def _raw(**overrides: object) -> RawOrder:
    fields: dict[str, object] = {
        "order_id": "O1",
        "user_id": "U000001",
        "product_id": "P00001",
        "quantity": 3,
        "unit_price": Decimal("10.00"),
        "discount": Decimal("0.10"),
        "status": "PAID",
        "created_at": DEFAULT_NOW - timedelta(days=1),
    }
    fields.update(overrides)
    return RawOrder(**fields)  # type: ignore[arg-type]


def _clean(order_date: date, region: str, status: str, amount: str) -> CleanOrder:
    return CleanOrder("O", "U", region, order_date, status, Decimal(amount))


# ---------------- transform ----------------


def test_transform_computes_amount_and_region() -> None:
    [clean] = transform([_raw()], {"U000001": _USER})
    assert clean.amount == Decimal("27.00")  # 3 * 10.00 * 0.90
    assert clean.region == "NORTH"
    assert clean.order_date == (DEFAULT_NOW - timedelta(days=1)).date()


def test_transform_rounds_half_up_to_cents() -> None:
    order = _raw(quantity=1, unit_price=Decimal("10.05"), discount=Decimal("0.05"))
    [clean] = transform([order], {"U000001": _USER})
    assert clean.amount == Decimal("9.55")  # 9.5475 -> 9.55


# ---------------- validate_stream ----------------


def test_validate_stream_splits_quarantine_and_lets_warnings_through() -> None:
    checker = QualityChecker(default_order_rules(["U000001"], DEFAULT_NOW))
    bad = _raw(order_id="BAD", quantity=0)
    warned = _raw(order_id="WARN", created_at=DEFAULT_NOW + timedelta(days=1))
    good = _raw(order_id="GOOD")
    quarantined: list[QuarantinedOrder] = []

    passed = list(validate_stream([bad, warned, good], checker, quarantined.append))

    assert [o.order_id for o in passed] == ["WARN", "GOOD"]
    assert [q.order.order_id for q in quarantined] == ["BAD"]
    assert any(v.severity is Severity.ERROR for v in quarantined[0].violations)


# ---------------- aggregate ----------------


def test_aggregate_counts_all_but_gmv_only_paid() -> None:
    d = date(2026, 8, 1)
    stats = aggregate(
        [
            _clean(d, "NORTH", "PAID", "10.00"),
            _clean(d, "NORTH", "PAID", "20.00"),
            _clean(d, "NORTH", "CANCELED", "99.00"),
            _clean(d, "SOUTH", "PENDING", "5.00"),
        ]
    )

    north, south = stats
    assert (north.region, north.order_count, north.paid_count) == ("NORTH", 3, 2)
    assert north.gmv == Decimal("30.00")
    assert north.avg_order_value == Decimal("15.00")
    assert (south.order_count, south.paid_count, south.gmv) == (1, 0, Decimal("0"))
    assert south.avg_order_value == Decimal("0.00")


def test_aggregate_output_is_sorted_by_date_then_region() -> None:
    d1, d2 = date(2026, 8, 1), date(2026, 8, 2)
    stats = aggregate(
        [
            _clean(d2, "A", "PAID", "1"),
            _clean(d1, "B", "PAID", "1"),
            _clean(d1, "A", "PAID", "1"),
        ]
    )
    assert [(s.order_date, s.region) for s in stats] == [(d1, "A"), (d1, "B"), (d2, "A")]


# ---------------- 端到端 ----------------


def _factory() -> TestDataFactory:
    return TestDataFactory(20260817, user_pool_size=50, product_pool_size=20)


def test_pipeline_matches_independent_expectation() -> None:
    """用 factory 标签独立推算期望值，与流水线结果对照"""
    factory = _factory()
    users = {u.user_id: u for u in factory.generate_users()}
    labeled = list(factory.generate_labeled_orders(10_000, dirty_ratio=0.05))

    # 除"未来时间"（只告警）外，所有脏数据都会被隔离
    survivors = [o for o, k in labeled if k is None or k is DirtyKind.FUTURE_TIME]
    expected_gmv = sum(
        (o.quantity * o.unit_price * (1 - o.discount)).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        for o in survivors
        if o.status == "PAID"
    )

    quarantined: list[QuarantinedOrder] = []
    result = run_orders_pipeline(
        factory.generate_orders(10_000, dirty_ratio=0.05),
        users.values(),
        now=DEFAULT_NOW,
        on_quarantine=quarantined.append,
    )

    assert result.quality.total_rows == 10_000
    assert len(quarantined) == result.quality.quarantined_rows == 10_000 - len(survivors)
    assert sum(s.order_count for s in result.stats) == len(survivors)
    assert sum(s.gmv for s in result.stats) == expected_gmv
    assert all(any(v.severity is Severity.ERROR for v in q.violations) for q in quarantined)


def test_pipeline_is_deterministic() -> None:
    def run() -> object:
        f = _factory()
        return run_orders_pipeline(
            f.generate_orders(2_000, dirty_ratio=0.1), f.generate_users(), now=DEFAULT_NOW
        )

    assert run() == run()


def test_pipeline_accepts_lazy_generator_without_materializing() -> None:
    f = _factory()
    lazy = (o for o in f.generate_orders(500))
    result = run_orders_pipeline(lazy, f.generate_users(), now=DEFAULT_NOW)
    assert result.quality.total_rows == 500
    assert result.quality.quarantined_rows == 0
