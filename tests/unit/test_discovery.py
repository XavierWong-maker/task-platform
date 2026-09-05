"""
discover_jobs 测试
用 tmp_path 动态创建一个临时包，模拟"新增一个 job 文件"的场景，
验证发现逻辑不需要事先知道文件名。
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType

import pytest

from task_platform.jobs.discovery import discover_jobs
from task_platform.registry import TaskRegistry


def _make_temp_package(tmp_path: Path, package_name: str) -> ModuleType:
    """在 tmp_path 下创建一个可 import 的临时包，并加入 sys.path"""
    pkg_dir = tmp_path / package_name
    pkg_dir.mkdir()
    (pkg_dir / "__init__.py").write_text("", encoding="utf-8")

    sys.path.insert(0, str(tmp_path))
    import importlib

    package = importlib.import_module(package_name)
    return package


@pytest.fixture
def temp_jobs_package(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """构造一个临时包 tmp_jobs_pkg，测试结束后清理 sys.modules / sys.path"""
    package_name = "tmp_jobs_pkg_for_test"
    package = _make_temp_package(tmp_path, package_name)

    yield tmp_path, package_name, package

    # 清理：避免污染后续测试的 sys.modules / sys.path
    for name in list(sys.modules):
        if name == package_name or name.startswith(f"{package_name}."):
            del sys.modules[name]
    if str(tmp_path) in sys.path:
        sys.path.remove(str(tmp_path))


def test_discover_finds_newly_added_job_file(temp_jobs_package) -> None:
    tmp_path, package_name, package = temp_jobs_package
    registry = TaskRegistry()

    job_file = tmp_path / package_name / "brand_new_job.py"
    job_file.write_text(
        "from task_platform.jobs.decorators import job\n"
        "from task_platform.registry import TaskRegistry\n"
        "import sys\n"
        f"_registry = sys.modules['{package_name}'].__dict__.setdefault('_test_registry', None)\n",
        encoding="utf-8",
    )
    # 直接把目标 registry 通过模块属性传进去比较绕；改用更直接的写法：
    job_file.write_text(
        "from task_platform.jobs.decorators import job\n\n"
        "from tests.unit.test_discovery import _shared_registry_for_temp_test\n\n"
        "@job('brand.new.job', registry=_shared_registry_for_temp_test)\n"
        "def brand_new() -> str:\n"
        "    return 'discovered'\n",
        encoding="utf-8",
    )

    global _shared_registry_for_temp_test
    _shared_registry_for_temp_test = registry

    discovered = discover_jobs(package)

    assert f"{package_name}.brand_new_job" in discovered
    assert registry.is_registered("brand.new.job")
    assert registry.get("brand.new.job").func() == "discovered"


def test_discover_skips_underscore_prefixed_modules(temp_jobs_package) -> None:
    tmp_path, package_name, package = temp_jobs_package

    (tmp_path / package_name / "_internal_helper.py").write_text("VALUE = 1\n", encoding="utf-8")
    (tmp_path / package_name / "visible_job.py").write_text("VALUE = 2\n", encoding="utf-8")

    discovered = discover_jobs(package)

    assert f"{package_name}.visible_job" in discovered
    assert f"{package_name}._internal_helper" not in discovered


def test_discover_returns_empty_list_for_empty_package(temp_jobs_package) -> None:
    _tmp_path, _package_name, package = temp_jobs_package
    discovered = discover_jobs(package)

    assert discovered == []
