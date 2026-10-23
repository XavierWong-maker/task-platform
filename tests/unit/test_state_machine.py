import pytest

from task_platform.domain.enums import JobStatus
from task_platform.domain.models import JobExecution
from task_platform.domain.state_machine import (
    InvalidStateTransitionError,
    can_transition,
    transition,
)


def test_happy_path_success() -> None:
    ex = JobExecution(job_id="job_1")
    transition(ex, JobStatus.QUEUED)
    transition(ex, JobStatus.RUNNING)
    transition(ex, JobStatus.SUCCESS)
    assert ex.status is JobStatus.SUCCESS


def test_retry_path() -> None:
    ex = JobExecution(job_id="job_1")
    transition(ex, JobStatus.QUEUED)
    transition(ex, JobStatus.RUNNING)
    transition(ex, JobStatus.RETRYING)
    transition(ex, JobStatus.QUEUED)
    transition(ex, JobStatus.RUNNING)
    transition(ex, JobStatus.FAILED)
    assert ex.status is JobStatus.FAILED


@pytest.mark.parametrize(
    "start,cancel_from",
    [
        (JobStatus.PENDING, JobStatus.PENDING),
        (JobStatus.QUEUED, JobStatus.QUEUED),
        (JobStatus.RUNNING, JobStatus.RUNNING),
    ],
)
def test_cancel_allowed_before_terminal(start: JobStatus, cancel_from: JobStatus) -> None:
    ex = JobExecution(job_id="job_1", status=start)
    transition(ex, JobStatus.CANCELED)
    assert ex.status is JobStatus.CANCELED


def test_success_cannot_go_back_to_running() -> None:
    ex = JobExecution(job_id="job_1", status=JobStatus.SUCCESS)
    assert can_transition(JobStatus.SUCCESS, JobStatus.RUNNING) is False
    with pytest.raises(InvalidStateTransitionError):
        transition(ex, JobStatus.RUNNING)


def test_terminal_states_are_final() -> None:
    for terminal in (JobStatus.SUCCESS, JobStatus.FAILED, JobStatus.CANCELED):
        for target in JobStatus:
            if target is terminal:
                continue
            assert can_transition(terminal, target) is False


def test_pending_cannot_skip_to_running() -> None:
    ex = JobExecution(job_id="job_1")
    with pytest.raises(InvalidStateTransitionError):
        transition(ex, JobStatus.RUNNING)


def test_running_can_become_recoverable_and_then_resume_or_end() -> None:
    assert can_transition(JobStatus.RUNNING, JobStatus.RECOVERABLE) is True
    assert can_transition(JobStatus.RECOVERABLE, JobStatus.QUEUED) is True
    assert can_transition(JobStatus.RECOVERABLE, JobStatus.FAILED) is True
    assert can_transition(JobStatus.RECOVERABLE, JobStatus.CANCELED) is True
    assert can_transition(JobStatus.RECOVERABLE, JobStatus.SUCCESS) is False
    assert can_transition(JobStatus.PENDING, JobStatus.RECOVERABLE) is False


def test_retrying_can_become_recoverable_but_not_succeed_directly() -> None:
    assert can_transition(JobStatus.RETRYING, JobStatus.RECOVERABLE) is True
    assert can_transition(JobStatus.RETRYING, JobStatus.SUCCESS) is False
