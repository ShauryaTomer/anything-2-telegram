import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest
from pyee.asyncio import AsyncIOEventEmitter

from anything2telegram.artifacts.storage import ArtifactStorage
from anything2telegram.domain import (
    ErrorInfo,
    JobPhase,
    JobStatus,
    SourceKind,
    StagedArtifact,
)
from anything2telegram.events import (
    ARTIFACT_PRODUCTION_FAILED,
    ARTIFACT_READY,
    ARTIFACT_UPLOAD_FAILED,
    ARTIFACT_UPLOADED,
    BATCH_CREATED,
    BATCH_JOBS_CREATED,
    JOB_QUEUED,
    JOB_STARTED,
    TELEGRAM_UNAVAILABLE,
    YOUTUBE_DOWNLOAD_REQUESTED,
    YOUTUBE_PLAYLIST_EXPANDED,
    YOUTUBE_PLAYLIST_EXPANSION_FAILED,
    YOUTUBE_PLAYLIST_EXPANSION_REQUESTED,
    ArtifactProductionFailed,
    ArtifactReady,
    ArtifactUploadFailed,
    ArtifactUploaded,
    BatchCreated,
    BatchJobsCreated,
    DownloadTarget,
    JobQueued,
    JobStarted,
    PlaylistExpanded,
    PlaylistExpansionFailed,
    PlaylistExpansionRequested,
    TelegramUnavailable,
    YouTubeDownloadRequested,
)
from anything2telegram.jobs.scheduler import JobScheduler, SchedulerError
from anything2telegram.jobs.tracker import JobTracker


NOW = datetime(2026, 7, 19, 8, 30, tzinfo=UTC)
JOB_1 = UUID("10000000-0000-0000-0000-000000000001")
JOB_2 = UUID("10000000-0000-0000-0000-000000000002")
JOB_3 = UUID("10000000-0000-0000-0000-000000000003")
BATCH = UUID("20000000-0000-0000-0000-000000000001")
ARTIFACT_1 = UUID("30000000-0000-0000-0000-000000000001")
ARTIFACT_2 = UUID("30000000-0000-0000-0000-000000000002")
DOWNLOAD_ERROR = ErrorInfo("download_failed", "Download failed")


async def flush_pump() -> None:
    await asyncio.sleep(0)


def time(step: int) -> datetime:
    return NOW + timedelta(seconds=step)


async def test_youtube_submission_queues_before_deferred_start() -> None:
    bus = AsyncIOEventEmitter()
    scheduler = JobScheduler(
        bus,
        id_factory=iter((JOB_1,)).__next__,
        clock=lambda: NOW,
    )
    facts: list[tuple[str, object]] = []
    bus.on(JOB_QUEUED, lambda event: facts.append((JOB_QUEUED, event)))
    bus.on(JOB_STARTED, lambda event: facts.append((JOB_STARTED, event)))
    bus.on(
        YOUTUBE_DOWNLOAD_REQUESTED,
        lambda event: facts.append((YOUTUBE_DOWNLOAD_REQUESTED, event)),
    )

    job_id = scheduler.submit_youtube_video(
        "https://example.test/watch?v=one"
    )

    assert job_id == JOB_1
    assert scheduler.pending_count == 1
    assert scheduler.active_id is None
    assert facts == [
        (
            JOB_QUEUED,
            JobQueued(
                JOB_1,
                None,
                SourceKind.YOUTUBE,
                "https://example.test/watch?v=one",
                None,
                NOW,
            ),
        )
    ]

    await flush_pump()

    assert scheduler.pending_count == 0
    assert scheduler.active_id == JOB_1
    assert facts[1:] == [
        (JOB_STARTED, JobStarted(JOB_1, JobPhase.PRODUCING, NOW)),
        (
            YOUTUBE_DOWNLOAD_REQUESTED,
            YouTubeDownloadRequested(
                JOB_1, "https://example.test/watch?v=one", NOW
            ),
        ),
    ]


async def test_synchronous_terminal_handler_never_starts_next_job_nested() -> None:
    bus = AsyncIOEventEmitter()
    nested_active_ids: list[UUID | None] = []

    def complete_immediately(event: YouTubeDownloadRequested) -> None:
        if event.job_id != JOB_1:
            return
        bus.emit(
            ARTIFACT_PRODUCTION_FAILED,
            ArtifactProductionFailed(JOB_1, None, DOWNLOAD_ERROR, NOW),
        )
        nested_active_ids.append(scheduler.active_id)

    bus.on(YOUTUBE_DOWNLOAD_REQUESTED, complete_immediately)
    scheduler = JobScheduler(
        bus,
        id_factory=iter((JOB_1, JOB_2)).__next__,
        clock=lambda: NOW,
    )
    started: list[UUID] = []
    bus.on(JOB_STARTED, lambda event: started.append(event.job_id))

    scheduler.submit_youtube_video("https://example.test/watch?v=one")
    scheduler.submit_youtube_video("https://example.test/watch?v=two")
    await flush_pump()

    assert nested_active_ids == [None]
    assert started[:1] == [JOB_1]

    await flush_pump()
    assert scheduler.active_id == JOB_2
    assert started == [JOB_1, JOB_2]


async def test_fifo_ignores_duplicate_and_stale_terminal_events() -> None:
    bus = AsyncIOEventEmitter()
    scheduler = JobScheduler(
        bus,
        id_factory=iter((JOB_1, JOB_2, JOB_3)).__next__,
        clock=lambda: NOW,
    )
    started: list[UUID] = []
    bus.on(JOB_STARTED, lambda event: started.append(event.job_id))

    for name in ("one", "two", "three"):
        scheduler.submit_youtube_video(f"https://example.test/watch?v={name}")
    await flush_pump()
    assert scheduler.active_id == JOB_1

    terminal_1 = ArtifactProductionFailed(JOB_1, None, DOWNLOAD_ERROR, NOW)
    bus.emit(ARTIFACT_PRODUCTION_FAILED, terminal_1)
    await flush_pump()
    assert scheduler.active_id == JOB_2

    bus.emit(ARTIFACT_PRODUCTION_FAILED, terminal_1)
    bus.emit(
        ARTIFACT_PRODUCTION_FAILED,
        ArtifactProductionFailed(JOB_3, None, DOWNLOAD_ERROR, NOW),
    )
    await flush_pump()
    assert scheduler.active_id == JOB_2
    assert started == [JOB_1, JOB_2]

    bus.emit(
        ARTIFACT_PRODUCTION_FAILED,
        ArtifactProductionFailed(JOB_2, None, DOWNLOAD_ERROR, NOW),
    )
    await flush_pump()
    assert scheduler.active_id == JOB_3
    assert started == [JOB_1, JOB_2, JOB_3]


async def test_playlist_children_are_deduped_and_inserted_at_queue_front() -> None:
    bus = AsyncIOEventEmitter()
    scheduler = JobScheduler(
        bus,
        id_factory=iter((BATCH, JOB_1, JOB_2, JOB_3)).__next__,
        clock=lambda: NOW,
    )
    facts: list[tuple[str, object]] = []
    for topic in (
        BATCH_CREATED,
        YOUTUBE_PLAYLIST_EXPANSION_REQUESTED,
        JOB_QUEUED,
        BATCH_JOBS_CREATED,
        JOB_STARTED,
    ):
        bus.on(topic, lambda event, topic=topic: facts.append((topic, event)))

    batch_id = scheduler.submit_playlist_expansion(
        "https://example.test/playlist"
    )
    later_job_id = scheduler.submit_youtube_video(
        "https://example.test/watch?v=later"
    )

    assert batch_id == BATCH
    assert later_job_id == JOB_1
    assert facts[0] == (
        BATCH_CREATED,
        BatchCreated(BATCH, "https://example.test/playlist", NOW),
    )

    await flush_pump()
    assert scheduler.active_id == BATCH
    assert (
        YOUTUBE_PLAYLIST_EXPANSION_REQUESTED,
        PlaylistExpansionRequested(
            BATCH, "https://example.test/playlist", NOW
        ),
    ) in facts

    before_expansion = list(facts)
    bus.emit(
        YOUTUBE_PLAYLIST_EXPANDED,
        PlaylistExpanded(
            BATCH,
            (
                DownloadTarget("first", "https://example.test/watch?v=first"),
                DownloadTarget("second", "https://example.test/watch?v=second"),
                DownloadTarget("first-alias", "https://example.test/watch?v=first"),
            ),
            2,
            NOW,
        ),
    )

    assert scheduler.active_id is None
    assert scheduler.pending_count == 3
    assert facts == before_expansion

    await flush_pump()

    child_1, child_2 = JOB_2, JOB_3
    child_facts = [event for topic, event in facts if topic == JOB_QUEUED]
    assert child_facts[-2:] == [
        JobQueued(
            child_1,
            BATCH,
            SourceKind.YOUTUBE,
            "https://example.test/watch?v=first",
            None,
            NOW,
        ),
        JobQueued(
            child_2,
            BATCH,
            SourceKind.YOUTUBE,
            "https://example.test/watch?v=second",
            None,
            NOW,
        ),
    ]
    assert (
        BATCH_JOBS_CREATED,
        BatchJobsCreated(BATCH, (child_1, child_2), 3, NOW),
    ) in facts
    assert scheduler.active_id == child_1

    bus.emit(
        ARTIFACT_PRODUCTION_FAILED,
        ArtifactProductionFailed(child_1, None, DOWNLOAD_ERROR, NOW),
    )
    await flush_pump()
    assert scheduler.active_id == child_2

    bus.emit(
        ARTIFACT_PRODUCTION_FAILED,
        ArtifactProductionFailed(child_2, None, DOWNLOAD_ERROR, NOW),
    )
    await flush_pump()
    assert scheduler.active_id == later_job_id


async def test_empty_playlist_emits_safe_failure_then_advances() -> None:
    bus = AsyncIOEventEmitter()
    scheduler = JobScheduler(
        bus,
        id_factory=iter((BATCH, JOB_1)).__next__,
        clock=lambda: NOW,
    )
    failures: list[PlaylistExpansionFailed] = []
    bus.on(YOUTUBE_PLAYLIST_EXPANSION_FAILED, failures.append)

    scheduler.submit_playlist_expansion("https://example.test/playlist")
    scheduler.submit_youtube_video("https://example.test/watch?v=later")
    await flush_pump()

    bus.emit(
        YOUTUBE_PLAYLIST_EXPANDED,
        PlaylistExpanded(BATCH, (), 4, NOW),
    )
    assert scheduler.active_id is None
    assert failures == []

    await flush_pump()

    assert failures == [
        PlaylistExpansionFailed(
            BATCH,
            ErrorInfo("playlist_empty", "Playlist contains no downloadable videos"),
            NOW,
        )
    ]
    assert scheduler.active_id == JOB_1


async def test_playlist_failure_advances_once_and_stale_failure_is_ignored() -> None:
    bus = AsyncIOEventEmitter()
    scheduler = JobScheduler(
        bus,
        id_factory=iter((BATCH, JOB_1, JOB_2)).__next__,
        clock=lambda: NOW,
    )
    scheduler.submit_playlist_expansion("https://example.test/playlist")
    scheduler.submit_youtube_video("https://example.test/watch?v=one")
    scheduler.submit_youtube_video("https://example.test/watch?v=two")
    await flush_pump()

    failed = PlaylistExpansionFailed(
        BATCH,
        ErrorInfo("playlist_failed", "Playlist expansion failed"),
        NOW,
    )
    bus.emit(YOUTUBE_PLAYLIST_EXPANSION_FAILED, failed)
    await flush_pump()
    assert scheduler.active_id == JOB_1

    bus.emit(YOUTUBE_PLAYLIST_EXPANSION_FAILED, failed)
    await flush_pump()
    assert scheduler.active_id == JOB_1


async def test_reserved_staged_upload_waits_for_fifo_turn(tmp_path: Path) -> None:
    bus = AsyncIOEventEmitter()
    storage = ArtifactStorage(tmp_path / "artifacts")
    scheduler = JobScheduler(
        bus,
        storage,
        id_factory=iter((JOB_1, JOB_2, ARTIFACT_1)).__next__,
        clock=lambda: NOW,
    )
    facts: list[tuple[str, object]] = []
    for topic in (JOB_QUEUED, JOB_STARTED, ARTIFACT_READY):
        bus.on(topic, lambda event, topic=topic: facts.append((topic, event)))

    scheduler.submit_youtube_video("https://example.test/watch?v=first")
    reservation = scheduler.reserve_local_upload(
        "local video.mp4", "video/mp4", "Local video"
    )
    reservation.destination.write_bytes(b"payload")
    staged = StagedArtifact(
        reservation.job_id,
        reservation.artifact_id,
        reservation.destination,
        reservation.filename,
        reservation.media_type,
        7,
        reservation.caption,
    )

    job_id = scheduler.enqueue_reserved_upload(staged)

    assert job_id == JOB_2
    assert reservation.job_id == JOB_2
    assert reservation.artifact_id == ARTIFACT_1
    assert not [event for topic, event in facts if topic == ARTIFACT_READY]

    await flush_pump()
    assert scheduler.active_id == JOB_1
    assert not [event for topic, event in facts if topic == ARTIFACT_READY]

    bus.emit(
        ARTIFACT_PRODUCTION_FAILED,
        ArtifactProductionFailed(JOB_1, None, DOWNLOAD_ERROR, NOW),
    )
    await flush_pump()

    assert scheduler.active_id == JOB_2
    assert facts[-2:] == [
        (JOB_STARTED, JobStarted(JOB_2, JobPhase.UPLOADING, NOW)),
        (
            ARTIFACT_READY,
            ArtifactReady(
                JOB_2,
                ARTIFACT_1,
                reservation.destination,
                reservation.filename,
                "video/mp4",
                7,
                "Local video",
                NOW,
            ),
        ),
    ]


@pytest.mark.parametrize(
    "failure_phase", ["dispatch", "pump", "start", "assembly"]
)
async def test_reserved_upload_commit_failure_stops_admission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_phase: str,
) -> None:
    class RecordingLogger:
        def __init__(self) -> None:
            self.errors: list[str] = []

        def error(self, message: str) -> None:
            self.errors.append(message)

    bus = AsyncIOEventEmitter()
    storage = ArtifactStorage(tmp_path / "artifacts")
    logger = RecordingLogger()
    scheduler = JobScheduler(
        bus,
        storage,
        id_factory=iter((JOB_1, ARTIFACT_1)).__next__,
        clock=lambda: NOW,
        logger=logger,
    )
    tracker = JobTracker()
    tracker.register(bus)
    if failure_phase in {"dispatch", "assembly"}:
        if failure_phase == "assembly":
            bus.on("error", scheduler.fail)
        bus.on(
            JOB_QUEUED,
            lambda _event: (_ for _ in ()).throw(RuntimeError("private")),
        )
    elif failure_phase == "start":
        bus.on(
            JOB_STARTED,
            lambda _event: (_ for _ in ()).throw(RuntimeError("private")),
        )
    reservation = scheduler.reserve_local_upload("video.mp4", None, None)
    reservation.destination.write_bytes(b"done")
    staged = StagedArtifact(
        JOB_1,
        ARTIFACT_1,
        reservation.destination,
        reservation.filename,
        None,
        4,
        None,
    )

    def fail_pump_schedule() -> None:
        raise RuntimeError("private loop detail")

    if failure_phase == "pump":
        monkeypatch.setattr(scheduler, "_request_pump", fail_pump_schedule)

    if failure_phase == "start":
        assert scheduler.enqueue_reserved_upload(staged) == JOB_1
        with pytest.raises(RuntimeError):
            scheduler._pump()
    else:
        expected_error = SchedulerError if failure_phase == "assembly" else RuntimeError
        with pytest.raises(expected_error):
            scheduler.enqueue_reserved_upload(staged)

    snapshot = tracker.get_job(JOB_1)
    assert snapshot is not None
    expected_status = (
        JobStatus.UPLOADING if failure_phase == "start" else JobStatus.WAITING
    )
    assert snapshot.status is expected_status
    assert scheduler.fatal
    assert not scheduler.accepting
    with pytest.raises(SchedulerError) as raised:
        scheduler.submit_youtube_video("https://example.test/watch?v=later")
    assert raised.value.code == "scheduler_unavailable"


async def test_reserved_upload_requires_completed_matching_local_write(
    tmp_path: Path,
) -> None:
    bus = AsyncIOEventEmitter()
    scheduler = JobScheduler(
        bus,
        ArtifactStorage(tmp_path / "artifacts"),
        id_factory=iter((JOB_1, ARTIFACT_1)).__next__,
        clock=lambda: NOW,
    )
    queued: list[JobQueued] = []
    bus.on(JOB_QUEUED, queued.append)
    reservation = scheduler.reserve_local_upload("video.mp4", None, None)
    staged = StagedArtifact(
        JOB_1,
        ARTIFACT_1,
        reservation.destination,
        reservation.filename,
        None,
        4,
        None,
    )

    try:
        scheduler.enqueue_reserved_upload(staged)
    except SchedulerError as error:
        assert error.code == "upload_not_staged"
    else:
        raise AssertionError("missing local write was accepted")

    assert queued == []
    assert scheduler.pending_count == 0

    reservation.destination.write_bytes(b"done")
    assert scheduler.enqueue_reserved_upload(staged) == JOB_1
    with pytest.raises(SchedulerError, match="reservation"):
        scheduler.enqueue_reserved_upload(staged)


async def test_telegram_unavailable_pauses_start_until_explicit_resume() -> None:
    bus = AsyncIOEventEmitter()
    scheduler = JobScheduler(
        bus,
        id_factory=iter((JOB_1,)).__next__,
        clock=lambda: NOW,
    )

    bus.emit(
        TELEGRAM_UNAVAILABLE,
        TelegramUnavailable(
            ErrorInfo("telegram_unavailable", "Telegram unavailable"), NOW
        ),
    )
    assert not scheduler.accepting
    unavailable_actions = [
        lambda: scheduler.submit_youtube_video("https://example.test/watch?v=new"),
        lambda: scheduler.submit_playlist_expansion("https://example.test/list"),
        lambda: scheduler.reserve_local_upload("video.mp4", None, None),
        lambda: scheduler.enqueue_reserved_upload(
            StagedArtifact(
                JOB_3,
                ARTIFACT_1,
                Path("/private/staged/video.mp4"),
                "video.mp4",
                None,
                1,
                None,
            )
        ),
    ]
    for action in unavailable_actions:
        with pytest.raises(SchedulerError) as raised:
            action()
        assert raised.value.code == "scheduler_unavailable"

    assert scheduler.paused
    assert scheduler.active_id is None
    assert scheduler.pending_count == 0

    scheduler.resume()
    scheduler.submit_youtube_video("https://example.test/watch?v=one")
    await flush_pump()

    assert not scheduler.paused
    assert scheduler.active_id == JOB_1


async def test_pause_keeps_active_owner_and_stop_prevents_future_start() -> None:
    bus = AsyncIOEventEmitter()
    scheduler = JobScheduler(
        bus,
        id_factory=iter((JOB_1, JOB_2, JOB_3)).__next__,
        clock=lambda: NOW,
    )
    scheduler.submit_youtube_video("https://example.test/watch?v=one")
    scheduler.submit_youtube_video("https://example.test/watch?v=two")
    await flush_pump()
    assert scheduler.active_id == JOB_1

    bus.emit(
        TELEGRAM_UNAVAILABLE,
        TelegramUnavailable(
            ErrorInfo("telegram_unavailable", "Telegram unavailable"), NOW
        ),
    )
    assert scheduler.active_id == JOB_1
    bus.emit(
        ARTIFACT_PRODUCTION_FAILED,
        ArtifactProductionFailed(JOB_1, None, DOWNLOAD_ERROR, NOW),
    )
    await flush_pump()
    assert scheduler.active_id is None

    scheduler.resume()
    scheduler.stop()
    await flush_pump()
    assert scheduler.stopped
    assert scheduler.active_id is None
    assert scheduler.pending_count == 1

    with pytest.raises(SchedulerError) as raised:
        scheduler.submit_youtube_video("https://example.test/watch?v=three")
    assert raised.value.code == "scheduler_unavailable"


@pytest.mark.parametrize("topic", [ARTIFACT_UPLOADED, ARTIFACT_UPLOAD_FAILED])
async def test_upload_terminal_events_advance_ready_owner(topic: str) -> None:
    bus = AsyncIOEventEmitter()
    scheduler = JobScheduler(
        bus,
        id_factory=iter((JOB_1, JOB_2)).__next__,
        clock=lambda: NOW,
    )
    scheduler.submit_youtube_video("https://example.test/watch?v=one")
    scheduler.submit_youtube_video("https://example.test/watch?v=two")
    await flush_pump()
    bus.emit(
        ARTIFACT_READY,
        ArtifactReady(
            JOB_1,
            ARTIFACT_1,
            Path("/private/artifacts/video.mp4"),
            "video.mp4",
            "video/mp4",
            7,
            None,
            NOW,
        ),
    )

    if topic == ARTIFACT_UPLOADED:
        event = ArtifactUploaded(JOB_1, ARTIFACT_1, -100123, 77, NOW)
    else:
        event = ArtifactUploadFailed(
            JOB_1,
            ARTIFACT_1,
            ErrorInfo("upload_failed", "Upload failed"),
            NOW,
        )
    bus.emit(topic, event)
    await flush_pump()

    assert scheduler.active_id == JOB_2


async def test_invalid_source_does_not_consume_id_or_mutate_queue() -> None:
    bus = AsyncIOEventEmitter()
    scheduler = JobScheduler(
        bus,
        id_factory=iter((JOB_1,)).__next__,
        clock=lambda: NOW,
    )

    with pytest.raises(ValueError, match="source_url"):
        scheduler.submit_youtube_video("  ")

    assert scheduler.pending_count == 0
    assert scheduler.submit_youtube_video(
        "https://example.test/watch?v=valid"
    ) == JOB_1


async def test_stop_from_synchronous_queued_listener_blocks_same_pump_start() -> None:
    bus = AsyncIOEventEmitter()
    scheduler = JobScheduler(
        bus,
        id_factory=iter((BATCH, JOB_1)).__next__,
        clock=lambda: NOW,
    )
    child_pending_counts: list[int] = []

    def stop_on_child(event: JobQueued) -> None:
        if event.batch_id is None:
            return
        child_pending_counts.append(scheduler.pending_count)
        scheduler.stop()

    bus.on(JOB_QUEUED, stop_on_child)
    scheduler.submit_playlist_expansion("https://example.test/playlist")
    await flush_pump()
    bus.emit(
        YOUTUBE_PLAYLIST_EXPANDED,
        PlaylistExpanded(
            BATCH,
            (DownloadTarget("one", "https://example.test/watch?v=one"),),
            0,
            NOW,
        ),
    )
    await flush_pump()

    assert child_pending_counts == [1]
    assert scheduler.stopped
    assert scheduler.active_id is None
    assert scheduler.pending_count == 1


@pytest.mark.parametrize("scheduler_first", [False, True])
async def test_tracker_and_scheduler_reject_invalid_matching_terminals(
    scheduler_first: bool,
) -> None:
    bus = AsyncIOEventEmitter()
    tracker = JobTracker()
    errors: list[Exception] = []
    bus.on("error", errors.append)
    ids = iter((JOB_1, JOB_2))
    clock_values = iter(time(step) for step in range(12))

    if scheduler_first:
        scheduler = JobScheduler(
            bus, id_factory=ids.__next__, clock=clock_values.__next__
        )
        tracker.register(bus)
    else:
        tracker.register(bus)
        scheduler = JobScheduler(
            bus, id_factory=ids.__next__, clock=clock_values.__next__
        )

    scheduler.submit_youtube_video("https://example.test/watch?v=one")
    scheduler.submit_youtube_video("https://example.test/watch?v=two")
    await flush_pump()

    bus.emit(
        ARTIFACT_PRODUCTION_FAILED,
        ArtifactProductionFailed(JOB_1, None, DOWNLOAD_ERROR, time(1)),
    )
    await flush_pump()
    assert scheduler.active_id == JOB_1
    job = tracker.get_job(JOB_1)
    assert job is not None
    assert job.status is JobStatus.PRODUCING

    bus.emit(
        ARTIFACT_READY,
        ArtifactReady(
            JOB_1,
            ARTIFACT_1,
            Path("/private/artifacts/video.mp4"),
            "video.mp4",
            "video/mp4",
            7,
            None,
            time(5),
        ),
    )
    bus.emit(
        ARTIFACT_PRODUCTION_FAILED,
        ArtifactProductionFailed(JOB_1, ARTIFACT_1, DOWNLOAD_ERROR, time(6)),
    )
    bus.emit(
        ARTIFACT_UPLOADED,
        ArtifactUploaded(JOB_1, ARTIFACT_2, -100123, 77, time(7)),
    )
    await flush_pump()

    assert scheduler.active_id == JOB_1
    job = tracker.get_job(JOB_1)
    assert job is not None
    assert job.status is JobStatus.UPLOADING
    assert len(errors) == 3

    bus.emit(
        ARTIFACT_UPLOADED,
        ArtifactUploaded(JOB_1, ARTIFACT_1, -100123, 78, time(8)),
    )
    await flush_pump()

    assert scheduler.active_id == JOB_2
    job = tracker.get_job(JOB_1)
    assert job is not None
    assert job.status is JobStatus.COMPLETED


async def test_cancel_unqueued_reservation_is_idempotent_and_cleans_storage(
    tmp_path: Path,
) -> None:
    storage = ArtifactStorage(tmp_path / "artifacts")
    scheduler = JobScheduler(
        AsyncIOEventEmitter(),
        storage,
        id_factory=iter((JOB_1, ARTIFACT_1)).__next__,
        clock=lambda: NOW,
    )
    reservation = scheduler.reserve_local_upload("video.mp4", None, None)
    job_directory = storage.root / str(JOB_1)
    assert job_directory.is_dir()

    assert scheduler.cancel_reserved_upload(JOB_1)
    assert not job_directory.exists()
    assert not scheduler.cancel_reserved_upload(JOB_1)


async def test_cancel_reserved_upload_does_not_cancel_queued_job(
    tmp_path: Path,
) -> None:
    bus = AsyncIOEventEmitter()
    tracker = JobTracker()
    tracker.register(bus)
    errors: list[Exception] = []
    bus.on("error", errors.append)
    storage = ArtifactStorage(tmp_path / "artifacts")
    scheduler = JobScheduler(
        bus,
        storage,
        id_factory=iter((JOB_1, JOB_2, ARTIFACT_1)).__next__,
        clock=lambda: NOW,
    )
    scheduler.submit_youtube_video("https://example.test/watch?v=active")
    reservation = scheduler.reserve_local_upload("video.mp4", None, None)
    reservation.destination.write_bytes(b"done")
    scheduler.enqueue_reserved_upload(
        StagedArtifact(
            JOB_2,
            ARTIFACT_1,
            reservation.destination,
            reservation.filename,
            None,
            4,
            None,
        )
    )
    await flush_pump()

    assert not scheduler.cancel_reserved_upload(JOB_2)
    assert scheduler.pending_count == 1
    assert reservation.destination.read_bytes() == b"done"
    job = tracker.get_job(JOB_2)
    assert job is not None
    assert job.status is JobStatus.WAITING

    bus.emit(
        ARTIFACT_PRODUCTION_FAILED,
        ArtifactProductionFailed(JOB_1, None, DOWNLOAD_ERROR, NOW),
    )
    await flush_pump()
    assert scheduler.active_id == JOB_2
    job = tracker.get_job(JOB_2)
    assert job is not None
    assert job.status is JobStatus.UPLOADING
    bus.emit(
        ARTIFACT_UPLOADED,
        ArtifactUploaded(JOB_2, ARTIFACT_1, -100123, 88, NOW),
    )
    assert errors == []
    job = tracker.get_job(JOB_2)
    assert job is not None
    assert job.status is JobStatus.COMPLETED


async def test_stop_cleans_reserved_and_waiting_staged_uploads(
    tmp_path: Path,
) -> None:
    bus = AsyncIOEventEmitter()
    storage = ArtifactStorage(tmp_path / "artifacts")
    scheduler = JobScheduler(
        bus,
        storage,
        id_factory=iter(
            (JOB_1, JOB_2, ARTIFACT_1, JOB_3, ARTIFACT_2)
        ).__next__,
        clock=lambda: NOW,
    )
    scheduler.submit_youtube_video("https://example.test/watch?v=active")
    waiting = scheduler.reserve_local_upload("waiting.mp4", None, None)
    waiting.destination.write_bytes(b"wait")
    scheduler.enqueue_reserved_upload(
        StagedArtifact(
            JOB_2,
            ARTIFACT_1,
            waiting.destination,
            waiting.filename,
            None,
            4,
            None,
        )
    )
    reserved = scheduler.reserve_local_upload("reserved.mp4", None, None)
    await flush_pump()
    assert scheduler.active_id == JOB_1

    scheduler.stop()

    assert scheduler.pending_count == 0
    assert not (storage.root / str(JOB_2)).exists()
    assert not (storage.root / str(JOB_3)).exists()
    assert not scheduler.cancel_reserved_upload(JOB_3)
    assert reserved.job_id == JOB_3


async def test_cancel_and_stop_never_delete_active_staged_upload(
    tmp_path: Path,
) -> None:
    bus = AsyncIOEventEmitter()
    storage = ArtifactStorage(tmp_path / "artifacts")
    scheduler = JobScheduler(
        bus,
        storage,
        id_factory=iter((JOB_1, ARTIFACT_1)).__next__,
        clock=lambda: NOW,
    )
    reservation = scheduler.reserve_local_upload("active.mp4", None, None)
    reservation.destination.write_bytes(b"live")
    scheduler.enqueue_reserved_upload(
        StagedArtifact(
            JOB_1,
            ARTIFACT_1,
            reservation.destination,
            reservation.filename,
            None,
            4,
            None,
        )
    )
    await flush_pump()

    assert not scheduler.cancel_reserved_upload(JOB_1)
    scheduler.stop()

    assert scheduler.active_id == JOB_1
    assert reservation.destination.read_bytes() == b"live"


async def test_stop_cleanup_continues_after_one_storage_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RecordingLogger:
        def __init__(self) -> None:
            self.errors: list[str] = []

        def error(self, message: str) -> None:
            self.errors.append(message)

    storage = ArtifactStorage(tmp_path / "artifacts")
    logger = RecordingLogger()
    scheduler = JobScheduler(
        AsyncIOEventEmitter(),
        storage,
        id_factory=iter((JOB_1, ARTIFACT_1, JOB_2, ARTIFACT_2)).__next__,
        clock=lambda: NOW,
        logger=logger,
    )
    scheduler.reserve_local_upload("first.mp4", None, None)
    scheduler.reserve_local_upload("second.mp4", None, None)
    original_delete = storage.delete_job_directory
    attempted: list[UUID] = []

    def fail_first(job_id: UUID) -> None:
        attempted.append(job_id)
        if job_id == JOB_1:
            raise RuntimeError("private path detail")
        original_delete(job_id)

    monkeypatch.setattr(storage, "delete_job_directory", fail_first)

    scheduler.stop()

    assert scheduler.stopped
    assert attempted == [JOB_1, JOB_2]
    assert (storage.root / str(JOB_1)).exists()
    assert not (storage.root / str(JOB_2)).exists()
    assert logger.errors == ["Local upload cleanup failed"]


async def test_playlist_derived_facts_respect_causal_time_during_clock_rollback(
) -> None:
    bus = AsyncIOEventEmitter()
    tracker = JobTracker()
    tracker.register(bus)
    errors: list[Exception] = []
    bus.on("error", errors.append)
    clock_values = iter((time(10), time(9), time(5), time(1), time(0)))
    scheduler = JobScheduler(
        bus,
        id_factory=iter((BATCH, JOB_1)).__next__,
        clock=clock_values.__next__,
    )
    facts: list[object] = []
    for topic in (
        YOUTUBE_PLAYLIST_EXPANSION_REQUESTED,
        JOB_QUEUED,
        BATCH_JOBS_CREATED,
        JOB_STARTED,
        YOUTUBE_DOWNLOAD_REQUESTED,
    ):
        bus.on(topic, facts.append)

    scheduler.submit_playlist_expansion("https://example.test/playlist")
    await flush_pump()
    bus.emit(
        YOUTUBE_PLAYLIST_EXPANDED,
        PlaylistExpanded(
            BATCH,
            (DownloadTarget("one", "https://example.test/watch?v=one"),),
            0,
            time(20),
        ),
    )
    await flush_pump()

    assert errors == []
    job = tracker.get_job(JOB_1)
    assert job is not None
    assert job.status is JobStatus.PRODUCING
    assert [event.occurred_at for event in facts] == [
        time(10),
        time(20),
        time(20),
        time(20),
        time(20),
    ]


async def test_staged_start_and_ready_respect_queued_time_during_clock_rollback(
    tmp_path: Path,
) -> None:
    bus = AsyncIOEventEmitter()
    tracker = JobTracker()
    tracker.register(bus)
    errors: list[Exception] = []
    bus.on("error", errors.append)
    storage = ArtifactStorage(tmp_path / "artifacts")
    scheduler = JobScheduler(
        bus,
        storage,
        id_factory=iter((JOB_1, ARTIFACT_1)).__next__,
        clock=iter((time(10), time(5), time(1))).__next__,
    )
    reservation = scheduler.reserve_local_upload("video.mp4", None, None)
    reservation.destination.write_bytes(b"done")
    starts: list[JobStarted] = []
    ready_facts: list[ArtifactReady] = []
    bus.on(JOB_STARTED, starts.append)
    bus.on(ARTIFACT_READY, ready_facts.append)

    scheduler.enqueue_reserved_upload(
        StagedArtifact(
            JOB_1,
            ARTIFACT_1,
            reservation.destination,
            reservation.filename,
            None,
            4,
            None,
        )
    )
    await flush_pump()

    assert errors == []
    assert starts[0].occurred_at == time(10)
    assert ready_facts[0].occurred_at == time(10)
    job = tracker.get_job(JOB_1)
    assert job is not None
    assert job.status is JobStatus.UPLOADING


async def test_staged_file_drift_fails_safely_without_artifact_ready(
    tmp_path: Path,
) -> None:
    bus = AsyncIOEventEmitter()
    tracker = JobTracker()
    tracker.register(bus)
    errors: list[Exception] = []
    bus.on("error", errors.append)
    storage = ArtifactStorage(tmp_path / "artifacts")
    scheduler = JobScheduler(
        bus,
        storage,
        id_factory=iter((JOB_1, JOB_2, ARTIFACT_1, JOB_3)).__next__,
        clock=lambda: NOW,
    )
    ready_facts: list[ArtifactReady] = []
    upload_failures: list[ArtifactUploadFailed] = []
    bus.on(ARTIFACT_READY, ready_facts.append)
    bus.on(ARTIFACT_UPLOAD_FAILED, upload_failures.append)

    scheduler.submit_youtube_video("https://example.test/watch?v=active")
    reservation = scheduler.reserve_local_upload("staged.mp4", None, None)
    reservation.destination.write_bytes(b"first")
    scheduler.enqueue_reserved_upload(
        StagedArtifact(
            JOB_2,
            ARTIFACT_1,
            reservation.destination,
            reservation.filename,
            None,
            5,
            None,
        )
    )
    scheduler.submit_youtube_video("https://example.test/watch?v=later")
    await flush_pump()
    reservation.destination.unlink()
    reservation.destination.write_bytes(b"other")

    bus.emit(
        ARTIFACT_PRODUCTION_FAILED,
        ArtifactProductionFailed(JOB_1, None, DOWNLOAD_ERROR, NOW),
    )
    await flush_pump()

    assert errors == []
    assert [event for event in ready_facts if event.job_id == JOB_2] == []
    assert upload_failures == [
        ArtifactUploadFailed(
            JOB_2,
            ARTIFACT_1,
            ErrorInfo("artifact_invalid", "Staged artifact is unavailable"),
            NOW,
        )
    ]
    job = tracker.get_job(JOB_2)
    assert job is not None
    assert job.status is JobStatus.FAILED
    assert not (storage.root / str(JOB_2)).exists()

    await flush_pump()
    assert scheduler.active_id == JOB_3
