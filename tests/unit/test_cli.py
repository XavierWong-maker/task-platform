from task_platform.cli.main import main


def test_run_success_returns_zero() -> None:
    assert main(["run", "demo-success"]) == 0


def test_run_failure_returns_nonzero() -> None:
    # demo-failure 第一次尝试会进入 RETRYING，同样不是 SUCCESS
    assert main(["run", "demo-failure"]) == 1
