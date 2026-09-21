import pytest

from task_platform.domain.enums import BackoffStrategy
from task_platform.domain.models import RetryPolicy


def test_fixed_delay_is_constant() -> None:
    policy = RetryPolicy(backoff=BackoffStrategy.FIXED, base_delay_seconds=2.0)
    assert [policy.delay_for_attempt(n) for n in (1, 2, 3)] == [2.0, 2.0, 2.0]


def test_exponential_delay_doubles_from_base() -> None:
    policy = RetryPolicy(
        backoff=BackoffStrategy.EXPONENTIAL, base_delay_seconds=1.0, max_delay_seconds=60.0
    )
    assert [policy.delay_for_attempt(n) for n in (1, 2, 3, 4)] == [1.0, 2.0, 4.0, 8.0]


def test_exponential_delay_is_capped_by_max() -> None:
    policy = RetryPolicy(
        backoff=BackoffStrategy.EXPONENTIAL, base_delay_seconds=1.0, max_delay_seconds=5.0
    )
    assert policy.delay_for_attempt(4) == 5.0


def test_delay_rejects_attempt_below_one() -> None:
    with pytest.raises(ValueError):
        RetryPolicy().delay_for_attempt(0)
