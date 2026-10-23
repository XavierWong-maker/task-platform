import threading

import pytest

from task_platform.domain.cancel import ActiveRuns, CancelToken, JobCanceledError


def test_token_check_raises_after_cancel() -> None:
    token = CancelToken()
    token.check()
    token.cancel()
    assert token.is_cancelled
    with pytest.raises(JobCanceledError):
        token.check()


def test_wait_is_interrupted_by_cancel() -> None:
    token = CancelToken()
    threading.Timer(0.05, token.cancel).start()
    assert token.wait(2.0) is True


def test_wait_times_out_when_not_cancelled() -> None:
    assert CancelToken().wait(0.01) is False


def test_active_runs_register_cancel_unregister() -> None:
    active = ActiveRuns()
    token = CancelToken()
    active.register("run_1", token)
    assert active.active_ids() == ["run_1"]
    assert active.cancel("run_1") is True and token.is_cancelled
    assert active.cancel("nope") is False
    active.unregister("run_1")
    assert active.active_ids() == []
