import os
import time
from dataclasses import dataclass

import pytest

from hhlake.mr import engine

WORK_S = 0.3
CRASH_AFTER_S = 0.1


@dataclass(frozen=True)
class Plan:
    crashes: frozenset


@dataclass
class Echo:
    index: int
    attempt: int
    pid: int
    seconds: float

    def describe(self):
        return f"echo {self.index}"


def flaky(plan, task, attempt):
    engine.announce("map", task.index, attempt)
    if (task.index, attempt) in plan.crashes:
        time.sleep(CRASH_AFTER_S)
        os._exit(engine.CRASH_EXIT_CODE)
    time.sleep(WORK_S)
    return Echo(task.index, attempt, os.getpid(), WORK_S)


def tasks(count):
    return [engine.MapTask(i, 0, 0) for i in range(count)]


def master(workers, lines):
    return engine.Master(workers, log=lambda line, **_: lines.append(line))


def charged(history):
    return {(a.index, a.attempt) for a in history if a.charged}


def test_crashed_mapper_is_retried_and_others_are_not_charged():
    lines = []
    with master(2, lines) as m:
        reports = m.run("map", flaky, Plan(frozenset({(3, 1)})), tasks(6))
        history = m.history
        failures = +m.failures
    assert [r.index for r in reports] == list(range(6))
    assert reports[3].attempt == 2
    crashed = [a for a in history if a.status == "crashed"]
    assert [(a.index, a.attempt, a.exitcode) for a in crashed] == [(3, 1, engine.CRASH_EXIT_CODE)]
    assert any(a.index == 3 and a.attempt == 2 and a.status == "ok" for a in history)
    assert charged(history) == {(3, 1)}
    assert all(a.status in engine.COLLATERAL for a in history if a.status not in ("ok", "crashed"))
    assert failures == {("map", 3): 1}
    assert any("crashed" in line and f"exit {engine.CRASH_EXIT_CODE}" in line for line in lines)
    assert any("pool broken" in line for line in lines)


def test_task_lost_three_times_by_other_crashes_still_succeeds():
    lines = []
    plan = Plan(frozenset({(0, 1), (1, 2), (2, 3)}))
    with master(1, lines) as m:
        reports = m.run("map", flaky, plan, tasks(4))
        history = m.history
        failures = +m.failures
    assert [r.index for r in reports] == [0, 1, 2, 3]
    assert reports[3].attempt == 4
    lost = [a for a in history if a.index == 3 and a.status != "ok"]
    assert len(lost) == 3
    assert not any(a.charged for a in lost)
    assert failures == {("map", 0): 1, ("map", 1): 1, ("map", 2): 1}


def test_task_crashing_on_every_attempt_fails_the_stage():
    lines = []
    plan = Plan(frozenset({(1, 1), (1, 2), (1, 3)}))
    m = master(2, lines)
    with m, pytest.raises(RuntimeError, match=f"map task 1 failed {engine.MAX_ATTEMPTS} times"):
        m.run("map", flaky, plan, tasks(4))
    crashed = [(a.index, a.attempt) for a in m.history if a.status == "crashed"]
    assert crashed == [(1, 1), (1, 2), (1, 3)]
    assert charged(m.history) == {(1, 1), (1, 2), (1, 3)}
    assert +m.failures == {("map", 1): engine.MAX_ATTEMPTS}
