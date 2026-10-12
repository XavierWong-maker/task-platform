from __future__ import annotations

from collections import Counter
from collections.abc import Iterator
from decimal import Decimal
from itertools import islice

import pytest

from task_platform.etl.factory import DEFAULT_NOW, TestDataFactory
from task_platform.etl.records import VALID_ORDER_STATUSES, DirtyKind, RawOrder


def _factory(seed: int = 20260817) -> TestDataFactory:
    return TestDataFactory(seed, user_pool_size=50, product_pool_size=20)


def test_same_seed_produces_identical_data() -> None:
    a = list(_factory().generate_orders(500, dirty_ratio=0.1))
    b = list(_factory().generate_orders(500, dirty_ratio=0.1))
    assert a == b


def test_different_seed_produces_different_data() -> None:
    a = list(_factory(1).generate_orders(100))
    b = list(_factory(2).generate_orders(100))
    assert a != b


def test_repeated_calls_on_same_factory_are_identical() -> None:
    f = _factory()
    assert list(f.generate_orders(50)) == list(f.generate_orders(50))
    assert list(f.generate_users()) == list(f.generate_users())


def test_prefix_is_stable_across_counts() -> None:
    f = _factory()
    small = list(f.generate_orders(100, dirty_ratio=0.2))
    large = list(islice(f.generate_orders(1000, dirty_ratio=0.2), 100))
    assert small == large


def test_orders_is_lazy_generator() -> None:
    assert isinstance(_factory().generate_orders(10**9), Iterator)  # 不会真的生成 10 亿条


def test_pool_sizes_and_id_formats() -> None:
    f = _factory()
    users = list(f.generate_users())
    products = list(f.generate_products())
    assert len(users) == 50 and users[0].user_id == "U000001"
    assert len(products) == 20 and products[0].product_id == "P00001"
    assert all(p.cost <= p.price for p in products)


def test_clean_orders_satisfy_all_rules() -> None:
    f = _factory()
    user_ids = {u.user_id for u in f.generate_users()}
    orders = list(f.generate_orders(1000, dirty_ratio=0.0))

    assert len({o.order_id for o in orders}) == 1000
    for o in orders:
        assert o.order_id is not None and o.created_at is not None
        assert o.quantity > 0 and o.unit_price >= 0
        assert o.status in VALID_ORDER_STATUSES
        assert o.user_id in user_ids
        assert o.created_at <= DEFAULT_NOW
        assert isinstance(o.unit_price, Decimal)


def test_dirty_ratio_is_roughly_respected_and_all_kinds_appear() -> None:
    labeled = list(_factory().generate_labeled_orders(20_000, dirty_ratio=0.03))
    kinds = Counter(k for _, k in labeled if k is not None)
    dirty_total = sum(kinds.values())

    assert 0.02 < dirty_total / 20_000 < 0.04
    assert set(kinds) == set(DirtyKind)


def _first_of(kind: DirtyKind) -> RawOrder:
    for order, k in _factory().generate_labeled_orders(5000, dirty_ratio=0.5):
        if k is kind:
            return order
    raise AssertionError(f"no {kind} generated")


def test_each_dirty_kind_really_breaks_its_rule() -> None:
    f = _factory()
    user_ids = {u.user_id for u in f.generate_users()}

    assert _first_of(DirtyKind.MISSING_ID).order_id is None
    assert _first_of(DirtyKind.BAD_QUANTITY).quantity <= 0
    assert _first_of(DirtyKind.NEGATIVE_PRICE).unit_price < 0
    assert _first_of(DirtyKind.BAD_STATUS).status not in VALID_ORDER_STATUSES
    assert _first_of(DirtyKind.ORPHAN_USER).user_id not in user_ids
    future = _first_of(DirtyKind.FUTURE_TIME).created_at
    assert future is not None and future > DEFAULT_NOW
    assert _first_of(DirtyKind.MISSING_TIME).created_at is None


def test_duplicate_id_really_duplicates_an_earlier_row() -> None:
    seen: set[str] = set()
    found = False
    for order, kind in _factory().generate_labeled_orders(5000, dirty_ratio=0.5):
        if kind is DirtyKind.DUPLICATE_ID:
            assert order.order_id in seen
            found = True
        if order.order_id is not None:
            seen.add(order.order_id)
    assert found


@pytest.mark.parametrize("ratio", [-0.1, 1.5])
def test_invalid_dirty_ratio_rejected(ratio: float) -> None:
    with pytest.raises(ValueError):
        list(_factory().generate_orders(1, dirty_ratio=ratio))


def test_naive_now_rejected() -> None:
    from datetime import datetime

    with pytest.raises(ValueError):
        TestDataFactory(1, now=datetime(2026, 1, 1))
