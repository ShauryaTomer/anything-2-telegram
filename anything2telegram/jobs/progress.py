"""In-memory registry of live transfer bytes, keyed by job id."""

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from uuid import UUID

from ..domain import JobPhase


# ponytail: matches tui.py's _LOG_INTERVAL_SECONDS. Two throttles exist because
# the registry and the headless log reporter are independent readers of the
# same callback; delete this one if they are ever unified.
_THROTTLE_SECONDS = 1.0


@dataclass(frozen=True)
class Progress:
    phase: JobPhase
    sent: int
    total: int  # 0 = unknown (download); never None


class ProgressWriter:
    def __init__(
        self, registry: "ProgressRegistry", job_id: UUID, phase: JobPhase
    ) -> None:
        self._registry = registry
        self._job_id = job_id
        self._phase = phase
        self._last_written = float("-inf")

    def update(self, sent: int, total: int) -> None:
        now = self._registry._clock()
        if now - self._last_written < _THROTTLE_SECONDS:
            return
        self._last_written = now
        self._registry._set(self._job_id, Progress(self._phase, sent, total))

    def done(self) -> None:
        self._registry._clear(self._job_id)


class ProgressRegistry:
    def __init__(self, clock: Callable[[], float] | None = None) -> None:
        self._clock = clock or (lambda: asyncio.get_running_loop().time())
        self._entries: dict[UUID, Progress] = {}

    def writer(self, job_id: UUID, phase: JobPhase) -> ProgressWriter:
        return ProgressWriter(self, job_id, phase)

    def get(self, job_id: UUID) -> Progress | None:
        return self._entries.get(job_id)

    def _set(self, job_id: UUID, progress: Progress) -> None:
        self._entries[job_id] = progress

    def _clear(self, job_id: UUID) -> None:
        self._entries.pop(job_id, None)
