from uuid import uuid4

from anything2telegram.domain import JobPhase
from anything2telegram.jobs.progress import ProgressRegistry


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_a_second_update_within_one_second_is_invisible_until_the_clock_advances() -> None:
    clock = FakeClock()
    registry = ProgressRegistry(clock=clock)
    job_id = uuid4()
    writer = registry.writer(job_id, JobPhase.PRODUCING)

    writer.update(10, 0)
    assert registry.get(job_id).sent == 10

    clock.advance(0.5)
    writer.update(20, 0)
    assert registry.get(job_id).sent == 10

    clock.advance(0.51)
    writer.update(20, 0)
    assert registry.get(job_id).sent == 20
