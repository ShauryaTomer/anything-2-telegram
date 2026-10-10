"""The wired web app: real bus, scheduler, tracker and SQLite, fake yt-dlp."""

import json
from collections.abc import Sequence
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest
from httpx import ASGITransport, AsyncClient
from pyee.asyncio import AsyncIOEventEmitter

from anything2telegram.api.jobs import create_jobs_app
from anything2telegram.artifacts.storage import ArtifactStorage
from anything2telegram.domain import (
    BatchRef,
    JobRef,
    JobSnapshot,
    ProcessResult,
    UploadReservation,
)
from anything2telegram.downloaders.youtube import YouTubeArtifactProducer
from anything2telegram.jobs.progress import ProgressRegistry
from anything2telegram.jobs.repositories import (
    BatchesRepository,
    BatchQueueRepository,
    JobQueueRepository,
    JobsRepository,
    open_database,
)
from anything2telegram.jobs.scheduler import JobScheduler
from anything2telegram.jobs.tracker import JobTracker
from tests.conftest import settings_for


class _RecordingScheduler:
    """Wraps a real JobScheduler to record what it was asked to submit."""

    def __init__(self, inner: JobScheduler) -> None:
        self._inner = inner
        self.calls: list[tuple[str, object]] = []

    @property
    def accepting(self) -> bool:
        return self._inner.accepting

    async def submit_video(self, url: str) -> JobRef:
        self.calls.append(("video", url))
        return await self._inner.submit_video(url)

    async def submit_playlist(self, url: str, offset: int = 0) -> BatchRef:
        self.calls.append(("playlist", (url, offset)))
        return await self._inner.submit_playlist(url, offset)

    async def reserve_local_upload(
        self, filename: str, media_type: str | None, caption: str | None
    ) -> UploadReservation:
        self.calls.append(("reserve", (filename, media_type, caption)))
        return await self._inner.reserve_local_upload(filename, media_type, caption)

    async def enqueue_reserved_upload(
        self, reservation: UploadReservation, size_bytes: int
    ) -> JobRef:
        self.calls.append(("enqueue", size_bytes))
        return await self._inner.enqueue_reserved_upload(reservation, size_bytes)

    async def enqueue_batch_retries(self, jobs: tuple[JobSnapshot, ...]) -> None:
        self.calls.append(("retry", tuple(job.id for job in jobs)))
        await self._inner.enqueue_batch_retries(jobs)

    async def retry_failed_batch(self, batch_id: UUID) -> tuple[JobSnapshot, ...]:
        self.calls.append(("retry_batch", batch_id))
        return await self._inner.retry_failed_batch(batch_id)

    async def retry_failed_job(self, job_id: UUID) -> JobSnapshot | None:
        self.calls.append(("retry_job", job_id))
        return await self._inner.retry_failed_job(job_id)

    async def cancel_reserved_upload(self, job_id: UUID) -> bool:
        return await self._inner.cancel_reserved_upload(job_id)


class FakeChannelRunner:
    """Stands in for yt-dlp on the flat-playlist listing calls.

    A `/playlists` URL answers with the canned channel entries; anything else
    is a playlist, answered with `counts[url]` fabricated video entries.
    """

    def __init__(self) -> None:
        self.playlists: list[dict[str, object]] = []
        self.counts: dict[str, int] = {}
        self.exit_code = 0
        self.calls: list[str] = []

    async def run(
        self, args: Sequence[str], _timeout_seconds: float
    ) -> ProcessResult:
        url = args[-1]
        self.calls.append(url)
        if url.endswith("/playlists"):
            stdout = "\n".join(json.dumps(entry) for entry in self.playlists)
            return ProcessResult(self.exit_code, stdout, "")
        videos = [
            json.dumps({"id": f"v{index:010d}", "title": f"Video {index}"})
            for index in range(self.counts.get(url, 0))
        ]
        return ProcessResult(0, "\n".join(videos), "")


def _ready(app) -> None:
    app.state.readiness = SimpleNamespace(is_accepting=lambda: True)
    app.state.telegram = SimpleNamespace(is_connected=True)


@pytest.fixture
async def wired_app(tmp_path: Path):
    """A real bus + scheduler + tracker, so a submit produces a real queue row."""
    app = create_jobs_app()
    bus = AsyncIOEventEmitter()
    storage = ArtifactStorage(tmp_path / "artifacts")
    conn = await open_database(tmp_path / "yt2tg.sqlite3")
    tracker = JobTracker(JobsRepository(conn), BatchesRepository(conn))
    tracker.register(bus)
    app.state.settings = SimpleNamespace(max_artifact_bytes=1024)
    app.state.storage = storage
    app.state.tracker = tracker
    app.state.scheduler = _RecordingScheduler(
        JobScheduler(
            bus,
            storage,
            JobQueueRepository(conn),
            BatchQueueRepository(conn),
            tracker,
        )
    )
    app.state.bus = bus
    app.state.channel_runner = FakeChannelRunner()
    # Its own bus: channel listing never emits, and the producer's download
    # handlers must stay off the bus the scheduler and tracker share.
    app.state.youtube = YouTubeArtifactProducer(
        AsyncIOEventEmitter(),
        storage,
        app.state.channel_runner,
        settings_for(tmp_path),
        ProgressRegistry(),
    )
    _ready(app)
    yield app
    await _drain(bus)
    await conn.close()


async def _drain(bus: AsyncIOEventEmitter) -> None:
    """Async handlers run as scheduled tasks, not inline; let them finish."""
    while not bus.complete:
        await bus.wait_for_complete()


async def _client(app) -> AsyncClient:
    transport = ASGITransport(app=app)
    return AsyncClient(transport=transport, base_url="http://service")
