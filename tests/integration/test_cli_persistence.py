from __future__ import annotations

from pathlib import Path

import pytest

from task_platform.cli.main import main
from task_platform.infrastructure.db.repositories import (
    ExecutionRepository,
    JobRepository,
    ScheduleRepository,
)
from task_platform.infrastructure.db.session import (
    create_db_engine,
    make_session_factory,
    session_scope,
)


@pytest.fixture(autouse=True)
def _db_url(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    url = f"sqlite:///{tmp_path / 'cli_test.db'}"
    monkeypatch.setenv("TASK_PLATFORM_DB_URL", url)
    return url


def _factory(url: str):  # noqa: ANN201 - sessionmaker 泛型标注省略，仅测试内部使用
    engine = create_db_engine(url)
    return make_session_factory(engine)


def test_job_id_is_stable_across_cli_invocations(_db_url: str) -> None:
    """两次独立的 main() 调用模拟"进程重启"：应该复用同一个 Job 行，
    而不是每次都插入新记录、生成新 id
    """
    assert main(["run", "demo-success"]) == 0
    assert main(["run", "demo-success"]) == 0

    factory = _factory(_db_url)
    with session_scope(factory) as s:
        job = JobRepository(s).get_by_name("demo-success")
        assert job is not None
        page = ExecutionRepository(s).list_paginated(job_id=job.id, page_size=10)
    assert page.total == 2  # 两次 run 各留下一条执行记录，Job 只有一行


def test_run_persists_execution_history(_db_url: str) -> None:
    assert main(["run", "demo-failure"]) == 1

    factory = _factory(_db_url)
    with session_scope(factory) as s:
        job = JobRepository(s).get_by_name("demo-failure")
        assert job is not None
        page = ExecutionRepository(s).list_paginated(job_id=job.id, page_size=10)

    assert page.total == 1
    assert page.items[0].error_message is not None


def test_cancel_persists_canceled_execution(_db_url: str) -> None:
    assert main(["cancel", "demo-slow"]) == 0

    factory = _factory(_db_url)
    with session_scope(factory) as s:
        job = JobRepository(s).get_by_name("demo-slow")
        assert job is not None
        page = ExecutionRepository(s).list_paginated(job_id=job.id, page_size=10)

    assert page.total == 1
    assert page.items[0].status.value == "CANCELED"


def test_schedule_command_persists_schedule(_db_url: str) -> None:
    assert main(["schedule", "demo-success", "--interval", "5"]) == 0

    factory = _factory(_db_url)
    with session_scope(factory) as s:
        job = JobRepository(s).get_by_name("demo-success")
        assert job is not None
        schedules = ScheduleRepository(s).list_by_job(job.id)

    assert len(schedules) == 1
    assert schedules[0].interval_seconds == 5.0


def test_serve_without_any_schedule_returns_error_without_blocking(_db_url: str) -> None:
    assert main(["serve"]) == 1


def test_disabling_job_survives_resync(_db_url: str) -> None:
    """用户手工在数据库里停用某任务后，再次调用 CLI（触发新一轮 sync）
    不应该把 enabled 重置回代码默认值 True
    """
    assert main(["list"]) == 0  # 先让 sync 把 demo-* 写入数据库

    factory = _factory(_db_url)
    with session_scope(factory) as s:
        repo = JobRepository(s)
        job = repo.get_by_name("demo-success")
        assert job is not None
        job.enabled = False
        repo.update(job)

    assert main(["show", "demo-success"]) == 0

    with session_scope(factory) as s:
        job = JobRepository(s).get_by_name("demo-success")
        assert job is not None
        assert job.enabled is False


def test_run_persists_logs_and_logs_command_shows_them(
    _db_url: str, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["run", "demo-failure"]) == 1

    factory = _factory(_db_url)
    with session_scope(factory) as s:
        job = JobRepository(s).get_by_name("demo-failure")
        assert job is not None
        run_id = ExecutionRepository(s).list_paginated(job_id=job.id).items[0].run_id

    capsys.readouterr()  # 清掉前面 run 的输出
    assert main(["logs", run_id]) == 0
    out = capsys.readouterr().out
    assert "job attempt started" in out
    assert "job attempt failed, will retry" in out


def test_logs_command_unknown_run_returns_error(_db_url: str) -> None:
    assert main(["logs", "run_doed_not_exist"]) == 1


def test_history_shows_persisted_executions_across_invocations(
    _db_url: str, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["run", "demo-success"]) == 0
    assert main(["run", "demo-success"]) == 0  # 第二次调用相当于"重启"后

    capsys.readouterr()
    assert main(["history", "demo-success"]) == 0
    out = capsys.readouterr().out
    assert out.count("SUCCESS") == 2
    assert "total=2" in out


def test_history_filters_by_status(_db_url: str, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["run", "demo-success"]) == 0
    assert main(["run", "demo-failure"]) == 1

    capsys.readouterr()
    assert main(["history", "--status", "SUCCESS"]) == 0
    out = capsys.readouterr().out
    assert "total=1" in out


def test_history_unknown_job_returns_error(_db_url: str) -> None:
    assert main(["history", "no-such-job"]) == 1
