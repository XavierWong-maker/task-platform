from __future__ import annotations

import signal
import threading

from task_platform.runtime import run_loop, stop_on_signals


def test_run_loop_repeats_until_stop_is_set() -> None:
    stop = threading.Event()
    calls: list[int] = []

    def step() -> None:
        calls.append(1)
        if len(calls) >= 3:
            stop.set()

    run_loop(step, interval=0.0, stop=stop)
    assert len(calls) == 3


def test_run_loop_survives_step_exceptions() -> None:
    stop = threading.Event()
    calls: list[int] = []

    def step() -> None:
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("redis down")
        stop.set()

    run_loop(step, interval=0.0, stop=stop, error_backoff=0.0)
    assert len(calls) == 2


def test_stop_on_signals_sets_event_and_restores_handler() -> None:
    before = signal.getsignal(signal.SIGINT)
    with stop_on_signals() as stop:
        signal.raise_signal(signal.SIGINT)
        assert stop.wait(1.0)
    assert signal.getsignal(signal.SIGINT) is before
