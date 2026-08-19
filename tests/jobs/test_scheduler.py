import asyncio
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from pyee.asyncio import AsyncIOEventEmitter

from anything2telegram.artifacts.storage import ArtifactStorage
from anything2telegram.domain import ErrorInfo, JobPhase, JobStatus, JobSnapshot, SourceKind
from anything2telegram.events import (
    ARTIFACT_PRODUCTION_FAILED,
    ARTIFACT_READY,
    ARTIFACT_UPLOAD_FAILED,
    ARTIFACT_UPLOADED,
    BATCH_CREATED,
    BATCH_JOBS_CREATED,
    ERROR,
    JOB_QUEUED,
    JOB_STARTED,
    TELEGRAM_UNAVAILABLE,
    YOUTUBE_DOWNLOAD_REQUESTED,
    YOUTUBE_PLAYLIST_EXPANDED,
    YOUTUBE_PLAYLIST_EXPANSION_FAILED,
    YOUTUBE_PLAYLIST_EXPANSION_REQUESTED,
    ArtifactProductionFailed,
    ArtifactReady,
    ArtifactUploaded,
    ArtifactUploadFailed,
    BatchCreated,
    BatchJobsCreated,
    DownloadTarget,
    JobQueued,
    JobStarted,
    PlaylistExpanded,
    PlaylistExpansionFailed,
    TelegramUnavailable,
)
from anything2telegram.jobs.repositories import (
    BatchQueueRepository,
    BatchesRepository,
    JobQueueRepository,
    JobQueueRow,
    PlaylistQueueRow,
    JobsRepository,
    open_database,
)
from anything2telegram.jobs.tracker import JobTracker
from anything2telegram.jobs.scheduler import JobScheduler, SchedulerError


def _stamp() -> datetime:
    return datetime.now(UTC)


async def _emit(
    bus: AsyncIOEventEmitter,
    topic: str,
    event: object,
) -> None:
    bus.emit(topic, event)
    await settle()


async def _seed_failed_job(
    bus: AsyncIOEventEmitter,
    batch_id: UUID,
    job_id: UUID,
) -> None:
    artifact_id = uuid4()

    await _emit(
        bus,
        BATCH_CREATED,
        BatchCreated(
            batch_id, "https://www.youtube.com/playlist?list=retries", _stamp()
        ),
    )
    await _emit(
        bus,
        JOB_QUEUED,
        JobQueued(
            job_id,
            batch_id,
            SourceKind.YOUTUBE,
            "https://www.youtube.com/watch?v=retry",
            None,
            _stamp(),
        ),
    )
    await _emit(
        bus,
        BATCH_JOBS_CREATED,
        BatchJobsCreated(batch_id, (job_id,), 0, _stamp()),
    )
    await _emit(bus, JOB_STARTED, JobStarted(job_id, JobPhase.PRODUCING, _stamp()))
    await _emit(
        bus,
        ARTIFACT_READY,
        ArtifactReady(
            job_id,
            artifact_id,
            Path("/tmp/retry.mp4"),
            "retry.mp4",
            "video/mp4",
            11,
            None,
            _stamp(),
        ),
    )
    await _emit(
        bus,
        ARTIFACT_UPLOAD_FAILED,
        ArtifactUploadFailed(
            job_id,
            artifact_id,
            ErrorInfo("upload_failed", "Upload failed"),
            _stamp(),
        ),
    )


VIDEO = "https://www.youtube.com/watch?v=aaaaaaaaaaa"
OTHER_VIDEO = "https://www.youtube.com/watch?v=bbbbbbbbbbb"
PLAYLIST = "https://www.youtube.com/playlist?list=PL123"

_TOPICS = (
    BATCH_CREATED,
    BATCH_JOBS_CREATED,
    JOB_QUEUED,
    JOB_STARTED,
    YOUTUBE_DOWNLOAD_REQUESTED,
    YOUTUBE_PLAYLIST_EXPANSION_REQUESTED,
    ARTIFACT_READY,
    ARTIFACT_UPLOAD_FAILED,
)


class Recorder:
    """Captures every fact the scheduler publishes, in order."""

    def __init__(self, bus: AsyncIOEventEmitter) -> None:
        self.facts: list[tuple[str, object]] = []
        for topic in _TOPICS:
            bus.on(topic, self._make_listener(topic))

    def _make_listener(self, topic: str):
        def listen(event: object) -> None:
            self.facts.append((topic, event))

        return listen

    def topics(self) -> list[str]:
        return [topic for topic, _ in self.facts]

    def only(self, topic: str) -> list[object]:
        return [event for name, event in self.facts if name == topic]


@pytest.fixture
def bus() -> AsyncIOEventEmitter:
    return AsyncIOEventEmitter()


@pytest.fixture
def recorder(bus: AsyncIOEventEmitter) -> Recorder:
    return Recorder(bus)


@pytest.fixture
def storage(tmp_path: Path) -> ArtifactStorage:
    return ArtifactStorage(tmp_path / "artifacts")


@pytest.fixture
async def scheduler(bus: AsyncIOEventEmitter, storage: ArtifactStorage, tmp_path: Path):
    conn = await open_database(tmp_path / "yt2tg.sqlite3")
    job_queue = JobQueueRepository(conn)
    batch_queue = BatchQueueRepository(conn)
    yield JobScheduler(bus, storage, job_queue, batch_queue)
    await conn.close()


@pytest.fixture
async def scheduler_with_tracker(
    bus: AsyncIOEventEmitter,
    storage: ArtifactStorage,
    tmp_path: Path,
):
    conn = await open_database(tmp_path / "yt2tg.sqlite3")
    job_queue = JobQueueRepository(conn)
    batch_queue = BatchQueueRepository(conn)
    tracker = JobTracker(JobsRepository(conn), BatchesRepository(conn))
    tracker.register(bus)
    yield JobScheduler(bus, storage, job_queue, batch_queue, tracker), tracker
    await conn.close()


async def settle() -> None:
    """Let the pump's queue reads/writes and bus dispatch finish.

    The pump runs as its own task, not one the bus tracks, and its DB calls
    hop through aiosqlite's background thread — plain ``sleep(0)`` ticks can
    race ahead of that thread, so give it real (if tiny) wall-clock time too.
    """
    for _ in range(10):
        await asyncio.sleep(0)
    for _ in range(10):
        await asyncio.sleep(0.001)


async def stage_upload(
    scheduler: JobScheduler, storage: ArtifactStorage, payload: bytes = b"abc"
):
    reservation = await scheduler.reserve_local_upload("clip.mp4", "video/mp4", "caption")
    reservation.destination.write_bytes(payload)
    return reservation


async def test_a_submitted_video_is_queued_then_started_and_requested(
    scheduler: JobScheduler, recorder: Recorder
) -> None:
    ref = await scheduler.submit_video(VIDEO)
    assert ref.status_url == f"/jobs/{ref.id}"
    assert recorder.topics() == [JOB_QUEUED]

    await settle()

    assert recorder.topics() == [JOB_QUEUED, JOB_STARTED, YOUTUBE_DOWNLOAD_REQUESTED]
    queued, started, requested = (event for _, event in recorder.facts)
    assert (queued.job_id, queued.source, queued.batch_id) == (ref.id, VIDEO, None)
    assert started.phase is JobPhase.PRODUCING
    assert requested.source_url == VIDEO
    assert queued.occurred_at <= started.occurred_at <= requested.occurred_at


async def test_only_one_job_runs_until_the_first_one_finishes(
    scheduler: JobScheduler, bus: AsyncIOEventEmitter, recorder: Recorder
) -> None:
    first = await scheduler.submit_video(VIDEO)
    second = await scheduler.submit_video(OTHER_VIDEO)
    await settle()

    assert [event.source_url for event in recorder.only(YOUTUBE_DOWNLOAD_REQUESTED)] == [
        VIDEO
    ]

    artifact_id = uuid4()
    bus.emit(
        ARTIFACT_READY,
        ArtifactReady(
            first.id, artifact_id, Path("/tmp/a.mp4"), "a.mp4", None, 3, None, _stamp()
        ),
    )
    bus.emit(
        ARTIFACT_UPLOADED, ArtifactUploaded(first.id, artifact_id, -1001, 7, _stamp())
    )
    await settle()

    assert [event.source_url for event in recorder.only(YOUTUBE_DOWNLOAD_REQUESTED)] == [
        VIDEO,
        OTHER_VIDEO,
    ]
    assert recorder.only(JOB_STARTED)[-1].job_id == second.id


async def test_a_production_failure_also_releases_the_slot(
    scheduler: JobScheduler, bus: AsyncIOEventEmitter, recorder: Recorder
) -> None:
    first = await scheduler.submit_video(VIDEO)
    await scheduler.submit_video(OTHER_VIDEO)
    await settle()

    bus.emit(
        ARTIFACT_PRODUCTION_FAILED,
        ArtifactProductionFailed(
            first.id, None, ErrorInfo("youtube_timeout", "Timed out"), _stamp()
        ),
    )
    await settle()

    assert len(recorder.only(YOUTUBE_DOWNLOAD_REQUESTED)) == 2


async def test_a_terminal_event_for_another_job_is_ignored(
    scheduler: JobScheduler, bus: AsyncIOEventEmitter, recorder: Recorder
) -> None:
    await scheduler.submit_video(VIDEO)
    await scheduler.submit_video(OTHER_VIDEO)
    await settle()

    bus.emit(
        ARTIFACT_PRODUCTION_FAILED,
        ArtifactProductionFailed(
            uuid4(), None, ErrorInfo("boom", "Boom"), _stamp()
        ),
    )
    await settle()

    assert len(recorder.only(YOUTUBE_DOWNLOAD_REQUESTED)) == 1


async def test_an_upload_outcome_for_a_stale_artifact_is_ignored(
    scheduler: JobScheduler, bus: AsyncIOEventEmitter, recorder: Recorder
) -> None:
    first = await scheduler.submit_video(VIDEO)
    await scheduler.submit_video(OTHER_VIDEO)
    await settle()
    bus.emit(
        ARTIFACT_READY,
        ArtifactReady(
            first.id, uuid4(), Path("/tmp/a.mp4"), "a.mp4", None, 3, None, _stamp()
        ),
    )

    bus.emit(
        ARTIFACT_UPLOAD_FAILED,
        ArtifactUploadFailed(
            first.id, uuid4(), ErrorInfo("boom", "Boom"), _stamp()
        ),
    )
    await settle()

    assert len(recorder.only(YOUTUBE_DOWNLOAD_REQUESTED)) == 1


async def test_a_playlist_expands_into_children_that_run_before_later_work(
    scheduler: JobScheduler, bus: AsyncIOEventEmitter, recorder: Recorder
) -> None:
    batch = await scheduler.submit_playlist(PLAYLIST, 2)
    later = await scheduler.submit_video(OTHER_VIDEO)
    await settle()

    requested = recorder.only(YOUTUBE_PLAYLIST_EXPANSION_REQUESTED)
    assert [(event.batch_id, event.offset) for event in requested] == [(batch.id, 2)]

    bus.emit(
        YOUTUBE_PLAYLIST_EXPANDED,
        PlaylistExpanded(
            batch.id,
            (DownloadTarget("aaaaaaaaaaa", VIDEO),),
            3,
            _stamp(),
        ),
    )
    await settle()

    created = recorder.only(BATCH_JOBS_CREATED)
    assert len(created) == 1
    assert created[0].skipped_entries == 3
    child_id = created[0].job_ids[0]
    child_queued = [
        event for event in recorder.only(JOB_QUEUED) if event.batch_id == batch.id
    ]
    assert [event.job_id for event in child_queued] == [child_id]
    # The child jumps the queue ahead of the video submitted after the playlist.
    assert [event.job_id for event in recorder.only(JOB_STARTED)] == [child_id]
    assert later.id not in {event.job_id for event in recorder.only(JOB_STARTED)}
    assert child_queued[0].title is None


async def test_a_playlist_expansions_target_titles_reach_the_job_queued_event(
    scheduler: JobScheduler, bus: AsyncIOEventEmitter, recorder: Recorder
) -> None:
    batch = await scheduler.submit_playlist(PLAYLIST)
    await settle()

    bus.emit(
        YOUTUBE_PLAYLIST_EXPANDED,
        PlaylistExpanded(
            batch.id,
            (DownloadTarget("aaaaaaaaaaa", VIDEO, title="Episode One"),),
            0,
            _stamp(),
        ),
    )
    await settle()

    [queued] = [
        event for event in recorder.only(JOB_QUEUED) if event.batch_id == batch.id
    ]
    assert queued.title == "Episode One"


async def test_a_failed_expansion_releases_the_slot(
    scheduler: JobScheduler, bus: AsyncIOEventEmitter, recorder: Recorder
) -> None:
    batch = await scheduler.submit_playlist(PLAYLIST)
    await scheduler.submit_video(VIDEO)
    await settle()

    bus.emit(
        YOUTUBE_PLAYLIST_EXPANSION_FAILED,
        PlaylistExpansionFailed(
            batch.id, ErrorInfo("playlist_empty", "Empty"), _stamp()
        ),
    )
    await settle()

    assert len(recorder.only(YOUTUBE_DOWNLOAD_REQUESTED)) == 1


async def test_a_reserved_upload_becomes_a_ready_artifact(
    scheduler: JobScheduler, storage: ArtifactStorage, recorder: Recorder
) -> None:
    reservation = await stage_upload(scheduler, storage)

    ref = await scheduler.enqueue_reserved_upload(reservation, 3)
    await settle()

    assert recorder.topics() == [JOB_QUEUED, JOB_STARTED, ARTIFACT_READY]
    queued, started, ready = (event for _, event in recorder.facts)
    assert queued.source == "clip.mp4"
    assert queued.staged_artifact is not None
    assert started.phase is JobPhase.UPLOADING
    assert (ready.job_id, ready.size_bytes, ready.caption) == (ref.id, 3, "caption")
    assert ready.local_path == reservation.destination


async def test_an_unknown_or_mismatched_reservation_is_refused(
    scheduler: JobScheduler, storage: ArtifactStorage
) -> None:
    reservation = await stage_upload(scheduler, storage)
    await scheduler.enqueue_reserved_upload(reservation, 3)

    with pytest.raises(SchedulerError) as error:
        await scheduler.enqueue_reserved_upload(reservation, 3)
    assert error.value.code == "invalid_reservation"


async def test_only_youtube_jobs_are_queued_for_batch_retry(
    scheduler: JobScheduler
) -> None:
    job_queue = scheduler._job_queue
    batch_queue = scheduler._batch_queue
    queued_id = uuid4()
    playlist_id = uuid4()
    retry_id = uuid4()

    await job_queue.enqueue(
        JobQueueRow(
            queued_id,
            kind="youtube",
            source_url="https://www.youtube.com/watch?v=existing",
            title="existing",
        )
    )
    await batch_queue.enqueue(
        PlaylistQueueRow(
            playlist_id,
            source_url="https://www.youtube.com/playlist?list=PLexisting",
            offset=0,
        )
    )
    failed_jobs = (
        JobSnapshot(
            id=retry_id,
            batch_id=None,
            source_kind=SourceKind.YOUTUBE,
            source="https://www.youtube.com/watch?v=retry",
            title="Episode One",
            status=JobStatus.FAILED,
            artifact_id=None,
            filename=None,
            size_bytes=123,
            telegram_chat_id=1,
            telegram_message_id=2,
            error=None,
            created_at=_stamp(),
            updated_at=_stamp(),
        ),
        JobSnapshot(
            id=uuid4(),
            batch_id=None,
            source_kind=SourceKind.LOCAL_UPLOAD,
            source="https://example.test/local.mp4",
            title=None,
            status=JobStatus.FAILED,
            artifact_id=None,
            filename="local.mp4",
            size_bytes=456,
            telegram_chat_id=3,
            telegram_message_id=4,
            error=None,
            created_at=_stamp(),
            updated_at=_stamp(),
        ),
    )
    await scheduler.enqueue_batch_retries(failed_jobs)

    queued = await job_queue.list_all()
    assert queued[0].job_id == retry_id
    assert queued[0].title == failed_jobs[0].title
    assert queued[0].source_url == failed_jobs[0].source
    assert queued[1].job_id == queued_id
    assert queued[1].title == "existing"
    assert len(queued) == 2
    batch_sequence = await batch_queue.peek_sequence()
    assert batch_sequence is not None
    queued_sequence = await job_queue.peek_sequence()
    assert queued_sequence is not None
    assert queued_sequence < batch_sequence


async def test_retry_failed_batch_requeues_only_retryable_failed_jobs(
    bus: AsyncIOEventEmitter,
    scheduler_with_tracker: tuple[JobScheduler, JobTracker],
) -> None:
    scheduler, tracker = scheduler_with_tracker
    batch_id = uuid4()
    failed_job = uuid4()
    await _seed_failed_job(bus, batch_id, failed_job)

    jobs = await scheduler.retry_failed_batch(batch_id)

    assert len(jobs) == 1
    assert jobs[0].id == failed_job
    tracked = await tracker.get_job(failed_job)
    assert tracked is not None
    assert tracked.status is JobStatus.WAITING
    assert tracked.artifact_id is None


async def test_retry_failed_batch_reverts_claim_if_queueing_fails(
    bus: AsyncIOEventEmitter,
    scheduler_with_tracker: tuple[JobScheduler, JobTracker],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scheduler, tracker = scheduler_with_tracker
    batch_id = uuid4()
    failed_job = uuid4()
    await _seed_failed_job(bus, batch_id, failed_job)

    async def _reject(_: tuple[JobSnapshot, ...]) -> None:
        raise SchedulerError("scheduler_unavailable", "Busy")

    monkeypatch.setattr(scheduler, "enqueue_batch_retries", _reject)

    with pytest.raises(SchedulerError):
        await scheduler.retry_failed_batch(batch_id)

    tracked = await tracker.get_job(failed_job)
    assert tracked is not None
    assert tracked.status is JobStatus.FAILED


async def test_a_reservation_whose_file_never_arrived_is_refused(
    scheduler: JobScheduler,
) -> None:
    reservation = await scheduler.reserve_local_upload("clip.mp4", None, None)

    with pytest.raises(SchedulerError) as error:
        await scheduler.enqueue_reserved_upload(reservation, 3)
    assert error.value.code == "upload_not_staged"


async def test_an_artifact_that_vanished_before_upload_fails_the_job(
    scheduler: JobScheduler, storage: ArtifactStorage, recorder: Recorder
) -> None:
    reservation = await stage_upload(scheduler, storage)
    ref = await scheduler.enqueue_reserved_upload(reservation, 3)
    reservation.destination.unlink()

    await settle()

    failures = recorder.only(ARTIFACT_UPLOAD_FAILED)
    assert [event.error.code for event in failures] == ["artifact_invalid"]
    assert recorder.only(ARTIFACT_READY) == []
    assert not (storage.root / str(ref.id)).exists()


async def test_cancelling_a_reservation_deletes_its_directory(
    scheduler: JobScheduler, storage: ArtifactStorage
) -> None:
    reservation = await stage_upload(scheduler, storage)

    assert await scheduler.cancel_reserved_upload(reservation.job_id) is True
    assert not (storage.root / str(reservation.job_id)).exists()
    assert await scheduler.cancel_reserved_upload(reservation.job_id) is False
    assert await scheduler.cancel_reserved_upload(uuid4()) is False


async def test_telegram_going_away_pauses_admission_and_the_pump(
    scheduler: JobScheduler, bus: AsyncIOEventEmitter, recorder: Recorder
) -> None:
    bus.emit(
        TELEGRAM_UNAVAILABLE,
        TelegramUnavailable(ErrorInfo("telegram_unavailable", "Gone"), _stamp()),
    )

    assert scheduler.accepting is False
    with pytest.raises(SchedulerError) as error:
        await scheduler.submit_video(VIDEO)
    assert error.value.code == "scheduler_unavailable"
    await settle()
    assert recorder.facts == []


async def test_stopping_releases_reservations_and_queued_uploads(
    scheduler: JobScheduler, storage: ArtifactStorage
) -> None:
    queued = await stage_upload(scheduler, storage)
    await scheduler.enqueue_reserved_upload(queued, 3)
    reserved = await stage_upload(scheduler, storage)

    await scheduler.stop()

    assert scheduler.accepting is False
    assert not (storage.root / str(queued.job_id)).exists()
    assert not (storage.root / str(reserved.job_id)).exists()


async def test_a_handler_failure_makes_the_scheduler_stop_accepting(
    scheduler: JobScheduler, bus: AsyncIOEventEmitter
) -> None:
    def explode(_event: object) -> None:
        raise RuntimeError("listener down")

    bus.on(JOB_QUEUED, explode)

    with pytest.raises(RuntimeError):
        await scheduler.submit_video(VIDEO)
    assert scheduler.accepting is False


async def test_failing_the_scheduler_drops_the_active_staged_upload(
    scheduler: JobScheduler, storage: ArtifactStorage
) -> None:
    reservation = await stage_upload(scheduler, storage)
    await scheduler.enqueue_reserved_upload(reservation, 3)
    await settle()

    scheduler.fail()
    await settle()

    assert scheduler.accepting is False
    assert not (storage.root / str(reservation.job_id)).exists()


async def test_the_error_topic_is_never_published_by_the_scheduler(
    scheduler: JobScheduler, bus: AsyncIOEventEmitter
) -> None:
    seen: list[object] = []
    bus.on(ERROR, seen.append)
    await scheduler.submit_video(VIDEO)
    await settle()
    assert seen == []
