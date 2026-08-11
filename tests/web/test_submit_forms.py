from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest
from httpx import ASGITransport, AsyncClient
from pyee.asyncio import AsyncIOEventEmitter

from anything2telegram.api.jobs import create_jobs_app
from anything2telegram.artifacts.storage import ArtifactStorage, ArtifactStorageError
from anything2telegram.domain import (
    BatchEntry,
    BatchRef,
    BatchSnapshot,
    BatchStatus,
    JobRef,
    JobSnapshot,
    JobStatus,
    SourceKind,
    StagedArtifact,
    UploadReservation,
)
from anything2telegram.jobs.scheduler import JobScheduler, SchedulerError
from anything2telegram.jobs.tracker import JobTracker

AT = datetime(2026, 7, 26, 12, 0, tzinfo=UTC)
VIDEO = "https://youtu.be/abcdefghijk"
PLAYLIST = "https://www.youtube.com/playlist?list=PL123"


class _RecordingScheduler:
    """Wraps a real JobScheduler to record what it was asked to submit."""

    def __init__(self, inner: JobScheduler) -> None:
        self._inner = inner
        self.calls: list[tuple[str, object]] = []

    @property
    def accepting(self) -> bool:
        return self._inner.accepting

    def submit_video(self, url: str) -> JobRef:
        self.calls.append(("video", url))
        return self._inner.submit_video(url)

    def submit_playlist(self, url: str, offset: int = 0) -> BatchRef:
        self.calls.append(("playlist", (url, offset)))
        return self._inner.submit_playlist(url, offset)

    def reserve_local_upload(
        self, filename: str, media_type: str | None, caption: str | None
    ) -> UploadReservation:
        self.calls.append(("reserve", (filename, media_type, caption)))
        return self._inner.reserve_local_upload(filename, media_type, caption)

    def enqueue_reserved_upload(
        self, reservation: UploadReservation, size_bytes: int
    ) -> JobRef:
        self.calls.append(("enqueue", size_bytes))
        return self._inner.enqueue_reserved_upload(reservation, size_bytes)

    def cancel_reserved_upload(self, job_id: UUID) -> bool:
        return self._inner.cancel_reserved_upload(job_id)


def _ready(app) -> None:
    app.state.readiness = SimpleNamespace(is_accepting=lambda: True)
    app.state.telegram = SimpleNamespace(is_connected=True)


@pytest.fixture
def wired_app(tmp_path: Path):
    """A real bus + scheduler + tracker, so a submit produces a real queue row."""
    app = create_jobs_app()
    bus = AsyncIOEventEmitter()
    storage = ArtifactStorage(tmp_path / "artifacts")
    tracker = JobTracker()
    tracker.register(bus)
    app.state.settings = SimpleNamespace(max_artifact_bytes=1024)
    app.state.storage = storage
    app.state.tracker = tracker
    app.state.scheduler = _RecordingScheduler(JobScheduler(bus, storage))
    _ready(app)
    return app


async def _client(app) -> AsyncClient:
    transport = ASGITransport(app=app)
    return AsyncClient(transport=transport, base_url="http://service")


def _empty_flash(body: str) -> bool:
    return '<div id="flash" hx-swap-oob="true"></div>' in body


async def test_youtube_video_submit_reaches_the_scheduler_and_shows_in_the_queue(
    wired_app,
) -> None:
    async with await _client(wired_app) as client:
        response = await client.post("/web/youtube", data={"url": VIDEO})

    assert response.status_code == 200
    body = response.text
    assert VIDEO in body
    assert _empty_flash(body)
    assert "<article>" not in body
    assert wired_app.state.scheduler.calls == [("video", VIDEO)]


async def test_youtube_playlist_submit_decrements_the_ui_offset(wired_app) -> None:
    async with await _client(wired_app) as client:
        response = await client.post(
            "/web/youtube", data={"url": PLAYLIST, "offset": "5"}
        )

    assert response.status_code == 200
    body = response.text
    assert PLAYLIST in body
    assert _empty_flash(body)
    assert wired_app.state.scheduler.calls == [("playlist", (PLAYLIST, 4))]


async def test_youtube_offset_defaults_to_one_and_is_inert_for_a_video(
    wired_app,
) -> None:
    async with await _client(wired_app) as client:
        response = await client.post("/web/youtube", data={"url": VIDEO})

    assert response.status_code == 200
    assert wired_app.state.scheduler.calls == [("video", VIDEO)]


async def test_an_unsupported_youtube_url_flashes_422_with_an_intact_queue(
    wired_app,
) -> None:
    async with await _client(wired_app) as client:
        response = await client.post(
            "/web/youtube", data={"url": "https://example.com/watch?v=abcdefghijk"}
        )

    assert response.status_code == 200
    body = response.text
    assert "<strong>422</strong>" in body
    assert "Not a supported YouTube URL." in body
    assert 'id="queue"' in body
    assert wired_app.state.scheduler.calls == []


async def test_youtube_playlist_submit_with_offset_one_skips_nothing(
    wired_app,
) -> None:
    async with await _client(wired_app) as client:
        response = await client.post(
            "/web/youtube", data={"url": PLAYLIST, "offset": "1"}
        )

    assert response.status_code == 200
    assert wired_app.state.scheduler.calls == [("playlist", (PLAYLIST, 0))]


async def test_youtube_submit_without_a_url_field_flashes_422_not_json(
    wired_app,
) -> None:
    async with await _client(wired_app) as client:
        response = await client.post("/web/youtube", data={"offset": "1"})

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    body = response.text
    assert "<strong>422</strong>" in body
    assert 'id="queue"' in body
    assert wired_app.state.scheduler.calls == []


@pytest.mark.parametrize("bad_offset", ["", "0", "-1", "abc", "1.5"])
async def test_youtube_submit_with_a_bad_offset_flashes_422_not_json(
    wired_app, bad_offset: str
) -> None:
    async with await _client(wired_app) as client:
        response = await client.post(
            "/web/youtube", data={"url": VIDEO, "offset": bad_offset}
        )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    body = response.text
    assert "<strong>422</strong>" in body
    assert 'id="queue"' in body
    assert wired_app.state.scheduler.calls == []


async def test_youtube_submit_while_not_ready_flashes_503(wired_app) -> None:
    wired_app.state.telegram = SimpleNamespace(is_connected=False)

    async with await _client(wired_app) as client:
        response = await client.post("/web/youtube", data={"url": VIDEO})

    assert response.status_code == 200
    body = response.text
    assert "<strong>503</strong>" in body
    assert "still connecting" in body
    assert 'id="queue"' in body
    assert wired_app.state.scheduler.calls == []


class _FakeSchedulerThatRefuses:
    """Reports ready (accepting) but rejects the submit itself."""

    accepting = True

    def submit_video(self, url: str) -> JobRef:
        raise SchedulerError("scheduler_unavailable", "Scheduler is unavailable")

    def submit_playlist(self, url: str, offset: int = 0) -> BatchRef:
        raise SchedulerError("scheduler_unavailable", "Scheduler is unavailable")


class _StaticTracker:
    def __init__(self, queue: tuple[object, ...]) -> None:
        self._queue = queue

    def list_queue(self) -> tuple[object, ...]:
        return self._queue


async def test_youtube_submit_when_scheduler_refuses_flashes_503(wired_app) -> None:
    existing = JobSnapshot(
        id=UUID(int=9),
        batch_id=None,
        source_kind=SourceKind.YOUTUBE,
        source="https://youtu.be/already-queued",
        title=None,
        status=JobStatus.WAITING,
        artifact_id=None,
        filename=None,
        size_bytes=None,
        telegram_chat_id=None,
        telegram_message_id=None,
        error=None,
        created_at=AT,
        updated_at=AT,
    )
    wired_app.state.scheduler = _FakeSchedulerThatRefuses()
    wired_app.state.tracker = _StaticTracker((existing,))

    async with await _client(wired_app) as client:
        response = await client.post("/web/youtube", data={"url": VIDEO})

    assert response.status_code == 200
    body = response.text
    assert "<strong>503</strong>" in body
    assert "https://youtu.be/already-queued" in body


async def test_an_upload_is_staged_and_shows_in_the_queue(wired_app) -> None:
    async with await _client(wired_app) as client:
        response = await client.post(
            "/web/upload", files={"file": ("clip.mp4", b"payload", "video/mp4")}
        )

    assert response.status_code == 200
    body = response.text
    assert "clip.mp4" in body
    assert _empty_flash(body)
    assert ("enqueue", 7) in wired_app.state.scheduler.calls


async def test_upload_while_not_ready_flashes_503(wired_app) -> None:
    wired_app.state.readiness = SimpleNamespace(is_accepting=lambda: False)

    async with await _client(wired_app) as client:
        response = await client.post(
            "/web/upload", files={"file": ("clip.mp4", b"payload", "video/mp4")}
        )

    assert response.status_code == 200
    body = response.text
    assert "<strong>503</strong>" in body
    assert 'id="queue"' in body
    assert wired_app.state.scheduler.calls == []


async def test_an_oversize_upload_flashes_413_before_staging(wired_app) -> None:
    wired_app.state.settings = SimpleNamespace(max_artifact_bytes=4)

    async with await _client(wired_app) as client:
        response = await client.post(
            "/web/upload", files={"file": ("clip.mp4", b"much too long", "video/mp4")}
        )

    assert response.status_code == 200
    body = response.text
    assert "<strong>413</strong>" in body
    assert "staging limit" in body
    assert 'id="queue"' in body
    assert wired_app.state.scheduler.calls == []


async def test_a_full_staging_disk_flashes_507_via_the_shared_op(
    wired_app, monkeypatch
) -> None:
    async def _stage_that_fails(*_args: object, **_kwargs: object) -> StagedArtifact:
        raise ArtifactStorageError("staging_disk_full", "Artifact storage is full")

    monkeypatch.setattr(wired_app.state.storage, "stage", _stage_that_fails)

    async with await _client(wired_app) as client:
        response = await client.post(
            "/web/upload", files={"file": ("clip.mp4", b"payload", "video/mp4")}
        )

    assert response.status_code == 200
    body = response.text
    assert "<strong>507</strong>" in body
    assert "Artifact storage is full" in body
    assert 'id="queue"' in body


async def test_a_malformed_upload_form_flashes_422(wired_app) -> None:
    async with await _client(wired_app) as client:
        response = await client.post("/web/upload", data={"note": "no file field"})

    assert response.status_code == 200
    body = response.text
    assert "<strong>422</strong>" in body
    assert 'id="queue"' in body
    assert wired_app.state.scheduler.calls == []


async def test_existing_json_youtube_route_is_unaffected(wired_app) -> None:
    async with await _client(wired_app) as client:
        response = await client.post("/jobs/youtube", json={"url": VIDEO})

    assert response.status_code == 202
    assert response.json()["type"] == "job"


async def test_existing_json_upload_route_is_unaffected(wired_app) -> None:
    async with await _client(wired_app) as client:
        response = await client.post(
            "/jobs/upload", files={"file": ("clip.mp4", b"payload", "video/mp4")}
        )

    assert response.status_code == 202
