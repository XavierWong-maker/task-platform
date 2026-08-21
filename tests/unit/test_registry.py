import pytest

from task_platform.jobs.dome_jobs import demo_failure, demo_success
from task_platform.registry import (
    DuplicateJobNameError,
    JobNotRegisteredError,
    TaskRegistry,
)


def test_register_and_get() -> None:
    registry = TaskRegistry()
    registry.register("demo.success", demo_success, description="总是成功")

    handler = registry.get("demo.success")
    assert handler.name == "demo.success"
    assert handler.func is demo_success
    assert handler.func() == "ok"


def test_duplicate_registration_raises() -> None:
    registry = TaskRegistry()
    registry.register("demo.success", demo_success)
    with pytest.raises(DuplicateJobNameError):
        registry.register("demo.success", demo_success)


def test_get_unknown_raises() -> None:
    registry = TaskRegistry()
    with pytest.raises(JobNotRegisteredError):
        registry.get("does.not.exist")


def test_list_names_preserves_order() -> None:
    registry = TaskRegistry()
    registry.register("a", demo_success)
    registry.register("b", demo_failure)
    assert registry.list_names() == ["a", "b"]


def test_is_registered() -> None:
    registry = TaskRegistry()
    assert registry.is_registered("demo.success") is False
    registry.register("demo.success", demo_success)
    assert registry.is_registered("demo.success") is True


def test_unregister() -> None:
    registry = TaskRegistry()
    registry.register("demo.success", demo_success)
    registry.unregister("demo.success")
    assert registry.is_registered("demo.success") is False


def test_demo_failure_raises_runtime_error() -> None:
    with pytest.raises(RuntimeError):
        demo_failure()
