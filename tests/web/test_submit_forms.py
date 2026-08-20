from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from anything2telegram.artifacts.storage import ArtifactStorageError
from anything2telegram.domain import (
    ErrorInfo,
    BatchRef,
    JobPhase,
    JobRef,
    JobSnapshot,
    JobStatus,
    SourceKind,
    StagedArtifact,
)
from anything2telegram.events import (
    ARTIFACT_UPLOAD_FAILED,
    ARTIFACT_UPLOADED,
    ARTIFACT_READY,
    BATCH_CREATED,
    BATCH_JOBS_CREATED,
    JOB_QUEUED,
    JOB_STARTED,
    ArtifactReady,
    ArtifactUploadFailed,
    ArtifactUploaded,
    BatchCreated,
    BatchJobsCreated,
    JobQueued,
    JobStarted,
)
from anything2telegram.jobs.scheduler import SchedulerError
from tests.web.conftest import _client, _drain

AT = datetime(2026, 7, 26, 12, 0, tzinfo=UTC)
VIDEO = "https://youtu.be/abcdefghijk"
PLAYLIST = "https://www.youtube.com/playlist?list=PL123"
UPLOAD_ERROR = ErrorInfo("upload_failed", "Upload failed")
ARTIFACT_1 = UUID("30000000-0000-0000-0000-000000000001")
ARTIFACT_2 = UUID("30000000-0000-0000-0000-000000000002")


def _time(step: int) -> datetime:
    return AT + timedelta(seconds=step)


async def _emit(bus, topic: str, event: object) -> None:
    bus.emit(topic, event)
    await _drain(bus)


async def _fail_job(bus, job_id: UUID, artifact_id: UUID, step: int) -> None:
    await _emit(bus, JOB_STARTED, JobStarted(job_id, JobPhase.PRODUCING, _time(step)))
    await _emit(
        bus,
        ARTIFACT_READY,
        ArtifactReady(
            job_id,
            artifact_id,
            Path(f"/tmp/{job_id}.mp4"),
            f"{job_id}.mp4",
            "video/mp4",
            111,
            None,
            _time(step + 1),
        ),
    )
    await _emit(
        bus,
        ARTIFACT_UPLOAD_FAILED,
        ArtifactUploadFailed(job_id, artifact_id, UPLOAD_ERROR, _time(step + 2)),
    )


def _empty_flash(body: str) -> bool:
    return '<div id="flash" hx-swap-oob="true"></div>' in body


async def test_youtube_video_submit_reaches_the_scheduler_and_shows_in_the_queue(
    wired_app,
) -> None:
    async with await _client(wired_app) as client:
        response = await client.post("/web/youtube", data={"url": VIDEO})
        assert response.status_code == 200
        body = response.text
        assert _empty_flash(body)
        assert "<article>" not in body

        # JOB_QUEUED's tracker handler runs as a scheduled task, not inline,
        # so the submitted job is not guaranteed to appear in this same
        # response body — it lands on the next 2s poll. Drain, then poll.
        await _drain(wired_app.state.bus)
        queue = await client.get("/web/queue")

    assert VIDEO in queue.text
    assert wired_app.state.scheduler.calls == [("video", VIDEO)]


async def test_youtube_playlist_submit_decrements_the_ui_offset(wired_app) -> None:
    async with await _client(wired_app) as client:
        response = await client.post(
            "/web/youtube", data={"url": PLAYLIST, "offset": "5"}
        )
        assert response.status_code == 200
        assert _empty_flash(response.text)

        await _drain(wired_app.state.bus)
        queue = await client.get("/web/queue")

    assert PLAYLIST in queue.text
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

    async def submit_video(self, url: str) -> JobRef:
        raise SchedulerError("scheduler_unavailable", "Scheduler is unavailable")

    async def submit_playlist(self, url: str, offset: int = 0) -> BatchRef:
        raise SchedulerError("scheduler_unavailable", "Scheduler is unavailable")


class _StaticTracker:
    def __init__(self, queue: tuple[object, ...]) -> None:
        self._queue = queue

    async def list_queue(self) -> tuple[object, ...]:
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
        assert _empty_flash(response.text)

        # See the video-submit test above: the queued job's tracker write
        # happens as a scheduled task, not inline within this response.
        await _drain(wired_app.state.bus)
        queue = await client.get("/web/queue")

    assert "clip.mp4" in queue.text
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


async def test_retry_failed_batch_posts_only_failed_jobs_and_resets_ui_state(
    wired_app,
) -> None:
    batch_id = uuid4()
    failed = uuid4()
    completed = uuid4()

    bus = wired_app.state.bus
    await _emit(bus, BATCH_CREATED, BatchCreated(batch_id, "https://youtu.be/playlist?list=retry", AT))
    await _emit(
        bus,
        JOB_QUEUED,
        JobQueued(
            failed,
            batch_id,
            SourceKind.YOUTUBE,
            "https://youtu.be/fail",
            None,
            _time(1),
        ),
    )
    await _emit(
        bus,
        JOB_QUEUED,
        JobQueued(
            completed,
            batch_id,
            SourceKind.YOUTUBE,
            "https://youtu.be/done",
            None,
            _time(2),
        ),
    )
    await _emit(
        bus,
        BATCH_JOBS_CREATED,
        BatchJobsCreated(batch_id, (failed, completed), 0, _time(3)),
    )
    await _emit(bus, JOB_STARTED, JobStarted(failed, JobPhase.PRODUCING, _time(4)))
    await _emit(
        bus,
        ARTIFACT_READY,
        ArtifactReady(
            failed,
            ARTIFACT_1,
            Path("/tmp/fail.mp4"),
            "fail.mp4",
            "video/mp4",
            111,
            None,
            _time(5),
        ),
    )
    await _emit(
        bus,
        ARTIFACT_UPLOAD_FAILED,
        ArtifactUploadFailed(
            failed,
            ARTIFACT_1,
            UPLOAD_ERROR,
            _time(6),
        ),
    )
    await _emit(bus, JOB_STARTED, JobStarted(completed, JobPhase.PRODUCING, _time(7)))
    await _emit(
        bus,
        ARTIFACT_READY,
        ArtifactReady(
            completed,
            ARTIFACT_2,
            Path("/tmp/done.mp4"),
            "done.mp4",
            "video/mp4",
            222,
            None,
            _time(8),
        ),
    )
    await _emit(
        bus,
        ARTIFACT_UPLOADED,
        ArtifactUploaded(completed, ARTIFACT_2, -100, 99, _time(9)),
    )

    async with await _client(wired_app) as client:
        queue = await client.get(f"/web/queue?open={batch_id}")
        assert "partially_completed" in queue.text
        assert 'data-icon="arrow-clockwise"' in queue.text

        response = await client.post(
            f"/web/batches/{batch_id}/retry-failed?open={batch_id}"
        )

    assert response.status_code == 200
    assert 'data-icon="arrow-clockwise"' not in response.text
    assert "partially_completed" not in response.text
    assert "No failed jobs to retry." not in response.text

    calls = wired_app.state.scheduler.calls[-1]
    assert calls[0] == "retry_batch"
    assert calls[1] == batch_id

    failed_job = await wired_app.state.tracker.get_job(failed)
    completed_job = await wired_app.state.tracker.get_job(completed)
    assert failed_job is not None and failed_job.status is not JobStatus.FAILED
    assert completed_job is not None and completed_job.status is JobStatus.COMPLETED


async def test_retry_failed_child_posts_only_that_job(wired_app) -> None:
    batch_id = uuid4()
    first = uuid4()
    second = uuid4()
    bus = wired_app.state.bus

    await _emit(
        bus,
        BATCH_CREATED,
        BatchCreated(batch_id, "https://youtu.be/playlist?list=child-retry", AT),
    )
    for step, job_id in enumerate((first, second), start=1):
        await _emit(
            bus,
            JOB_QUEUED,
            JobQueued(
                job_id,
                batch_id,
                SourceKind.YOUTUBE,
                f"https://youtu.be/{job_id}",
                None,
                _time(step),
            ),
        )
    await _emit(
        bus,
        BATCH_JOBS_CREATED,
        BatchJobsCreated(batch_id, (first, second), 0, _time(3)),
    )
    await _fail_job(bus, first, ARTIFACT_1, 4)
    await _fail_job(bus, second, ARTIFACT_2, 7)

    async with await _client(wired_app) as client:
        queue = await client.get(f"/web/queue?open={batch_id}")
        assert f'hx-post="/web/jobs/{first}/retry-failed?open={batch_id}"' in queue.text
        assert 'hx-swap="outerHTML show:none focus-scroll:false"' in queue.text

        response = await client.post(
            f"/web/jobs/{first}/retry-failed?open={batch_id}"
        )

    assert response.status_code == 200
    assert "No failed job to retry." not in response.text
    assert wired_app.state.scheduler.calls[-1] == ("retry_job", first)

    first_job = await wired_app.state.tracker.get_job(first)
    second_job = await wired_app.state.tracker.get_job(second)
    assert first_job is not None and first_job.status is not JobStatus.FAILED
    assert second_job is not None and second_job.status is JobStatus.FAILED


async def test_retry_failed_batch_requires_failed_children(wired_app) -> None:
    batch_id = uuid4()
    child = uuid4()

    bus = wired_app.state.bus
    await _emit(bus, BATCH_CREATED, BatchCreated(batch_id, "https://youtu.be/playlist?list=none", AT))
    await _emit(
        bus,
        JOB_QUEUED,
        JobQueued(
            child,
            batch_id,
            SourceKind.YOUTUBE,
            "https://youtu.be/good",
            None,
            _time(1),
        ),
    )
    await _emit(bus, BATCH_JOBS_CREATED, BatchJobsCreated(batch_id, (child,), 0, _time(3)))
    await _emit(bus, JOB_STARTED, JobStarted(child, JobPhase.PRODUCING, _time(4)))
    await _emit(
        bus,
        ARTIFACT_READY,
        ArtifactReady(
            child,
            ARTIFACT_1,
            Path("/tmp/good.mp4"),
            "good.mp4",
            "video/mp4",
            111,
            None,
            _time(5),
        ),
    )
    await _emit(
        bus,
        ARTIFACT_UPLOADED,
        ArtifactUploaded(child, ARTIFACT_1, -101, 123, _time(6)),
    )

    async with await _client(wired_app) as client:
        response = await client.post(f"/web/batches/{batch_id}/retry-failed")

    assert response.status_code == 200
    assert "<strong>422</strong>" in response.text
    assert "No failed jobs to retry." in response.text


async def test_retry_failed_batch_icon_exists_for_fully_failed_playlist(
    wired_app,
) -> None:
    batch_id = uuid4()
    first = uuid4()
    second = uuid4()

    bus = wired_app.state.bus
    await _emit(
        bus,
        BATCH_CREATED,
        BatchCreated(batch_id, "https://youtu.be/playlist?list=failed", AT),
    )
    await _emit(
        bus,
        JOB_QUEUED,
        JobQueued(
            first,
            batch_id,
            SourceKind.YOUTUBE,
            "https://youtu.be/first",
            None,
            _time(1),
        ),
    )
    await _emit(
        bus,
        JOB_QUEUED,
        JobQueued(
            second,
            batch_id,
            SourceKind.YOUTUBE,
            "https://youtu.be/second",
            None,
            _time(2),
        ),
    )
    await _emit(
        bus,
        BATCH_JOBS_CREATED,
        BatchJobsCreated(batch_id, (first, second), 0, _time(3)),
    )
    await _emit(bus, JOB_STARTED, JobStarted(first, JobPhase.PRODUCING, _time(4)))
    await _emit(
        bus,
        ARTIFACT_READY,
        ArtifactReady(
            first,
            ARTIFACT_1,
            Path("/tmp/first.mp4"),
            "first.mp4",
            "video/mp4",
            111,
            None,
            _time(5),
        ),
    )
    await _emit(
        bus,
        ARTIFACT_UPLOAD_FAILED,
        ArtifactUploadFailed(
            first,
            ARTIFACT_1,
            UPLOAD_ERROR,
            _time(6),
        ),
    )
    await _emit(bus, JOB_STARTED, JobStarted(second, JobPhase.PRODUCING, _time(7)))
    await _emit(
        bus,
        ARTIFACT_READY,
        ArtifactReady(
            second,
            ARTIFACT_2,
            Path("/tmp/second.mp4"),
            "second.mp4",
            "video/mp4",
            222,
            None,
            _time(8),
        ),
    )
    await _emit(
        bus,
        ARTIFACT_UPLOAD_FAILED,
        ArtifactUploadFailed(
            second,
            ARTIFACT_2,
            UPLOAD_ERROR,
            _time(9),
        ),
    )

    async with await _client(wired_app) as client:
        queue = await client.get(f"/web/queue?open={batch_id}")
        assert "failed" in queue.text
        assert 'data-icon="arrow-clockwise"' in queue.text

        response = await client.post(
            f"/web/batches/{batch_id}/retry-failed?open={batch_id}"
        )

    assert response.status_code == 200
    assert 'data-icon="arrow-clockwise"' not in response.text
    assert "No failed jobs to retry." not in response.text

    calls = wired_app.state.scheduler.calls[-1]
    assert calls[0] == "retry_batch"
    assert calls[1] == batch_id

    first_job = await wired_app.state.tracker.get_job(first)
    second_job = await wired_app.state.tracker.get_job(second)
    assert first_job is not None and first_job.status is not JobStatus.FAILED
    assert second_job is not None and second_job.status is not JobStatus.FAILED


async def test_retry_failed_batch_unknown_batch_reports_422(wired_app) -> None:
    unknown = uuid4()
    async with await _client(wired_app) as client:
        response = await client.post(f"/web/batches/{unknown}/retry-failed")

    assert response.status_code == 200
    assert "<strong>422</strong>" in response.text
    assert "Batch not found." in response.text
