from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest
from pyee.asyncio import AsyncIOEventEmitter

from anything2telegram.domain import (
    BatchEntry,
    BatchSnapshot,
    BatchStatus,
    ErrorInfo,
    JobPhase,
    JobSnapshot,
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
    YOUTUBE_PLAYLIST_EXPANDED,
    YOUTUBE_PLAYLIST_EXPANSION_FAILED,
    ArtifactProductionFailed,
    ArtifactReady,
    ArtifactUploadFailed,
    ArtifactUploaded,
    BatchCreated,
    BatchJobsCreated,
    JobQueued,
    JobStarted,
    PlaylistExpanded,
    PlaylistExpansionFailed,
)
from anything2telegram.jobs.repositories import (
    BatchesRepository,
    JobsRepository,
    open_database,
)
from anything2telegram.jobs.tracker import (
    InvalidJobTransition,
    JobTracker,
    TrackingError,
)


NOW = datetime(2026, 7, 19, 8, 30, tzinfo=UTC)
JOB_1 = UUID("10000000-0000-0000-0000-000000000001")
JOB_2 = UUID("10000000-0000-0000-0000-000000000002")
JOB_3 = UUID("10000000-0000-0000-0000-000000000003")
BATCH = UUID("20000000-0000-0000-0000-000000000001")
ARTIFACT_1 = UUID("30000000-0000-0000-0000-000000000001")
ARTIFACT_2 = UUID("30000000-0000-0000-0000-000000000002")
DOWNLOAD_ERROR = ErrorInfo("download_failed", "Download failed")
UPLOAD_ERROR = ErrorInfo("upload_failed", "Upload failed")


def time(step: int) -> datetime:
    return NOW + timedelta(seconds=step)


async def _settle(bus: AsyncIOEventEmitter) -> None:
    """Async handlers run as scheduled tasks, not inline; let them finish."""
    while not bus.complete:
        await bus.wait_for_complete()


async def _build_tracker(tmp_path: Path) -> tuple[JobTracker, object]:
    conn = await open_database(tmp_path / "yt2tg.sqlite3")
    tracker = JobTracker(JobsRepository(conn), BatchesRepository(conn))
    return tracker, conn


@pytest.fixture
async def tracked_bus(tmp_path: Path) -> tuple[JobTracker, AsyncIOEventEmitter, list[Exception]]:
    tracker, conn = await _build_tracker(tmp_path)
    bus = AsyncIOEventEmitter()
    tracker.register(bus)
    errors: list[Exception] = []
    bus.on("error", errors.append)
    yield tracker, bus, errors
    await conn.close()


async def emit_valid(bus: AsyncIOEventEmitter, errors: list[Exception], topic: str, event: object) -> None:
    before = len(errors)
    assert bus.emit(topic, event)
    await _settle(bus)
    assert errors[before:] == []


async def emit_invalid(
    bus: AsyncIOEventEmitter,
    errors: list[Exception],
    topic: str,
    event: object,
    error_type: type[Exception] = TrackingError,
) -> Exception:
    before = len(errors)
    assert bus.emit(topic, event)
    await _settle(bus)
    assert len(errors) == before + 1
    error = errors[-1]
    assert isinstance(error, error_type)
    return error


def youtube_queued(
    job_id: UUID = JOB_1,
    *,
    batch_id: UUID | None = None,
    occurred_at: datetime = NOW,
) -> JobQueued:
    return JobQueued(
        job_id,
        batch_id,
        SourceKind.YOUTUBE,
        "https://example.test/watch?v=one",
        None,
        occurred_at,
    )


def staged_queued(
    job_id: UUID = JOB_1,
    *,
    artifact_id: UUID = ARTIFACT_1,
    occurred_at: datetime = NOW,
) -> JobQueued:
    staged = StagedArtifact(
        job_id,
        artifact_id,
        Path("/private/staging/video.mp4"),
        "video.mp4",
        "video/mp4",
        123,
        "Video",
    )
    return JobQueued(
        job_id,
        None,
        SourceKind.LOCAL_UPLOAD,
        "video.mp4",
        staged,
        occurred_at,
    )


def ready(
    job_id: UUID = JOB_1,
    *,
    artifact_id: UUID = ARTIFACT_1,
    occurred_at: datetime = NOW,
) -> ArtifactReady:
    return ArtifactReady(
        job_id,
        artifact_id,
        Path("/private/artifacts/video.mp4"),
        "video.mp4",
        "video/mp4",
        123,
        "Video",
        occurred_at,
    )


def uploaded(
    job_id: UUID = JOB_1,
    *,
    artifact_id: UUID = ARTIFACT_1,
    message_id: int = 77,
    occurred_at: datetime = NOW,
) -> ArtifactUploaded:
    return ArtifactUploaded(job_id, artifact_id, -100123, message_id, occurred_at)


async def create_batch_and_jobs(
    bus: AsyncIOEventEmitter,
    errors: list[Exception],
    job_ids: tuple[UUID, ...] = (JOB_1, JOB_2),
    *,
    skipped_entries: int = 0,
) -> None:
    await emit_valid(
        bus,
        errors,
        BATCH_CREATED,
        BatchCreated(BATCH, "https://example.test/playlist", time(0)),
    )
    for index, job_id in enumerate(job_ids, start=1):
        await emit_valid(
            bus,
            errors,
            JOB_QUEUED,
            youtube_queued(job_id, batch_id=BATCH, occurred_at=time(index)),
        )
    await emit_valid(
        bus,
        errors,
        BATCH_JOBS_CREATED,
        BatchJobsCreated(BATCH, job_ids, skipped_entries, time(3)),
    )


async def move_job(
    bus: AsyncIOEventEmitter,
    errors: list[Exception],
    job_id: UUID,
    status: JobStatus,
    step: int,
) -> int:
    if status is JobStatus.WAITING:
        return step
    await emit_valid(bus, errors, JOB_STARTED, JobStarted(job_id, JobPhase.PRODUCING, time(step)))
    step += 1
    if status is JobStatus.PRODUCING:
        return step
    if status is JobStatus.FAILED:
        await emit_valid(
            bus,
            errors,
            ARTIFACT_PRODUCTION_FAILED,
            ArtifactProductionFailed(job_id, None, DOWNLOAD_ERROR, time(step)),
        )
        return step + 1
    artifact_id = ARTIFACT_1 if job_id == JOB_1 else ARTIFACT_2
    await emit_valid(
        bus,
        errors,
        ARTIFACT_READY,
        ready(job_id, artifact_id=artifact_id, occurred_at=time(step)),
    )
    step += 1
    if status is JobStatus.UPLOADING:
        return step
    if status is JobStatus.COMPLETED:
        await emit_valid(
            bus,
            errors,
            ARTIFACT_UPLOADED,
            uploaded(job_id, artifact_id=artifact_id, occurred_at=time(step)),
        )
        return step + 1
    raise AssertionError(f"unsupported target status: {status}")


async def create_job_at_status(
    bus: AsyncIOEventEmitter,
    errors: list[Exception],
    status: JobStatus,
) -> None:
    await emit_valid(bus, errors, JOB_QUEUED, youtube_queued())
    if status is JobStatus.WAITING:
        return
    await emit_valid(
        bus,
        errors,
        JOB_STARTED,
        JobStarted(JOB_1, JobPhase.PRODUCING, time(1)),
    )
    if status is JobStatus.PRODUCING:
        return
    await emit_valid(bus, errors, ARTIFACT_READY, ready(occurred_at=time(2)))
    if status is JobStatus.UPLOADING:
        return
    if status is JobStatus.COMPLETED:
        await emit_valid(
            bus,
            errors,
            ARTIFACT_UPLOADED,
            uploaded(occurred_at=time(3)),
        )
        return
    await emit_valid(
        bus,
        errors,
        ARTIFACT_UPLOAD_FAILED,
        ArtifactUploadFailed(JOB_1, ARTIFACT_1, UPLOAD_ERROR, time(3)),
    )


def terminal_fact(kind: str, step: int) -> tuple[str, object]:
    if kind == "production_failed":
        return (
            ARTIFACT_PRODUCTION_FAILED,
            ArtifactProductionFailed(JOB_1, None, DOWNLOAD_ERROR, time(step)),
        )
    if kind == "uploaded":
        return ARTIFACT_UPLOADED, uploaded(occurred_at=time(step))
    return (
        ARTIFACT_UPLOAD_FAILED,
        ArtifactUploadFailed(JOB_1, ARTIFACT_1, UPLOAD_ERROR, time(step)),
    )


async def test_unknown_reads_return_none(tmp_path: Path) -> None:
    tracker, conn = await _build_tracker(tmp_path)

    assert await tracker.get_job(JOB_1) is None
    assert await tracker.get_batch(BATCH) is None
    await conn.close()


async def test_job_queued_creates_exact_waiting_snapshot(tracked_bus) -> None:
    tracker, bus, errors = tracked_bus
    await emit_valid(bus, errors, JOB_QUEUED, youtube_queued())

    assert await tracker.get_job(JOB_1) == JobSnapshot(
        id=JOB_1,
        batch_id=None,
        source_kind=SourceKind.YOUTUBE,
        source="https://example.test/watch?v=one",
        title=None,
        status=JobStatus.WAITING,
        artifact_id=None,
        filename=None,
        size_bytes=None,
        telegram_chat_id=None,
        telegram_message_id=None,
        error=None,
        created_at=NOW,
        updated_at=NOW,
    )


async def test_staged_job_keeps_artifact_metadata_but_stays_waiting(tracked_bus) -> None:
    tracker, bus, errors = tracked_bus
    await emit_valid(bus, errors, JOB_QUEUED, staged_queued())

    snapshot = await tracker.get_job(JOB_1)
    assert snapshot is not None
    assert snapshot.status is JobStatus.WAITING
    assert snapshot.artifact_id == ARTIFACT_1
    assert snapshot.filename == "video.mp4"
    assert snapshot.size_bytes == 123
    assert snapshot.created_at == snapshot.updated_at == NOW


async def test_youtube_job_success_lifecycle_has_exact_fields_and_timestamps(tracked_bus) -> None:
    tracker, bus, errors = tracked_bus
    await emit_valid(bus, errors, JOB_QUEUED, youtube_queued())
    await emit_valid(bus, errors, JOB_STARTED, JobStarted(JOB_1, JobPhase.PRODUCING, time(1)))
    producing = await tracker.get_job(JOB_1)
    assert producing is not None
    assert producing.status is JobStatus.PRODUCING
    assert producing.updated_at == time(1)

    await emit_valid(bus, errors, ARTIFACT_READY, ready(occurred_at=time(2)))
    uploading = await tracker.get_job(JOB_1)
    assert uploading is not None
    assert uploading.status is JobStatus.UPLOADING
    assert uploading.artifact_id == ARTIFACT_1
    assert uploading.filename == "video.mp4"
    assert uploading.size_bytes == 123
    assert uploading.updated_at == time(2)

    await emit_valid(bus, errors, ARTIFACT_UPLOADED, uploaded(occurred_at=time(3)))
    assert await tracker.get_job(JOB_1) == JobSnapshot(
        id=JOB_1,
        batch_id=None,
        source_kind=SourceKind.YOUTUBE,
        source="https://example.test/watch?v=one",
        title=None,
        status=JobStatus.COMPLETED,
        artifact_id=ARTIFACT_1,
        filename="video.mp4",
        size_bytes=123,
        telegram_chat_id=-100123,
        telegram_message_id=77,
        error=None,
        created_at=NOW,
        updated_at=time(3),
    )


async def test_staged_job_success_lifecycle_confirms_artifact_ready(tracked_bus) -> None:
    tracker, bus, errors = tracked_bus
    await emit_valid(bus, errors, JOB_QUEUED, staged_queued())
    await emit_valid(bus, errors, JOB_STARTED, JobStarted(JOB_1, JobPhase.UPLOADING, time(1)))
    await emit_valid(bus, errors, ARTIFACT_READY, ready(occurred_at=time(2)))
    await emit_valid(bus, errors, ARTIFACT_UPLOADED, uploaded(occurred_at=time(3)))

    snapshot = await tracker.get_job(JOB_1)
    assert snapshot is not None
    assert snapshot.status is JobStatus.COMPLETED
    assert snapshot.telegram_message_id == 77
    assert snapshot.updated_at == time(3)


@pytest.mark.parametrize("is_staged", [False, True])
async def test_exact_duplicate_artifact_ready_is_idempotent(tracked_bus, is_staged) -> None:
    tracker, bus, errors = tracked_bus
    queued = staged_queued() if is_staged else youtube_queued()
    phase = JobPhase.UPLOADING if is_staged else JobPhase.PRODUCING
    event = ready(occurred_at=time(2))
    await emit_valid(bus, errors, JOB_QUEUED, queued)
    await emit_valid(bus, errors, JOB_STARTED, JobStarted(JOB_1, phase, time(1)))
    await emit_valid(bus, errors, ARTIFACT_READY, event)
    first = await tracker.get_job(JOB_1)

    await emit_valid(bus, errors, ARTIFACT_READY, event)
    assert await tracker.get_job(JOB_1) == first


@pytest.mark.parametrize(
    "changes",
    [
        {"artifact_id": ARTIFACT_2},
        {"local_path": Path("/private/artifacts/other.mp4")},
        {"filename": "other.mp4"},
        {"media_type": "application/octet-stream"},
        {"size_bytes": 124},
        {"caption": "Other"},
        {"occurred_at": time(3)},
    ],
)
@pytest.mark.parametrize("is_staged", [False, True])
async def test_changed_second_artifact_ready_is_invalid(
    tracked_bus, changes, is_staged
) -> None:
    tracker, bus, errors = tracked_bus
    event = ready(occurred_at=time(2))
    queued = staged_queued() if is_staged else youtube_queued()
    phase = JobPhase.UPLOADING if is_staged else JobPhase.PRODUCING
    await emit_valid(bus, errors, JOB_QUEUED, queued)
    await emit_valid(
        bus,
        errors,
        JOB_STARTED,
        JobStarted(JOB_1, phase, time(1)),
    )
    await emit_valid(bus, errors, ARTIFACT_READY, event)
    first = await tracker.get_job(JOB_1)

    await emit_invalid(
        bus,
        errors,
        ARTIFACT_READY,
        replace(event, **changes),
        InvalidJobTransition,
    )
    assert await tracker.get_job(JOB_1) == first


@pytest.mark.parametrize(
    ("topic", "event"),
    [
        (JOB_STARTED, JobStarted(JOB_1, JobPhase.PRODUCING, NOW)),
        (ARTIFACT_READY, ready()),
        (
            ARTIFACT_PRODUCTION_FAILED,
            ArtifactProductionFailed(JOB_1, None, DOWNLOAD_ERROR, NOW),
        ),
        (ARTIFACT_UPLOADED, uploaded()),
        (
            ARTIFACT_UPLOAD_FAILED,
            ArtifactUploadFailed(JOB_1, ARTIFACT_1, UPLOAD_ERROR, NOW),
        ),
    ],
)
async def test_job_events_reject_missing_job(tracked_bus, topic, event) -> None:
    _, bus, errors = tracked_bus
    await emit_invalid(bus, errors, topic, event)


async def test_duplicate_job_id_is_invalid_and_preserves_first_job(tracked_bus) -> None:
    tracker, bus, errors = tracked_bus
    await emit_valid(bus, errors, JOB_QUEUED, youtube_queued())
    error = await emit_invalid(
        bus,
        errors,
        JOB_QUEUED,
        staged_queued(occurred_at=time(1)),
    )

    assert "10000000" not in str(error)
    assert "/private" not in str(error)
    assert (await tracker.get_job(JOB_1)).source_kind is SourceKind.YOUTUBE  # type: ignore[union-attr]


async def test_batched_job_requires_existing_batch(tracked_bus) -> None:
    tracker, bus, errors = tracked_bus

    await emit_invalid(bus, errors, JOB_QUEUED, youtube_queued(batch_id=BATCH))
    assert await tracker.get_job(JOB_1) is None


async def test_job_started_only_accepts_waiting_job(tracked_bus) -> None:
    tracker, bus, errors = tracked_bus
    await emit_valid(bus, errors, JOB_QUEUED, youtube_queued())
    await emit_valid(bus, errors, JOB_STARTED, JobStarted(JOB_1, JobPhase.PRODUCING, time(1)))

    await emit_invalid(
        bus,
        errors,
        JOB_STARTED,
        JobStarted(JOB_1, JobPhase.UPLOADING, time(2)),
        InvalidJobTransition,
    )
    assert (await tracker.get_job(JOB_1)).status is JobStatus.PRODUCING  # type: ignore[union-attr]


@pytest.mark.parametrize(
    ("queued", "phase"),
    [
        (youtube_queued(), JobPhase.UPLOADING),
        (staged_queued(), JobPhase.PRODUCING),
    ],
)
async def test_job_started_rejects_phase_for_wrong_source(tracked_bus, queued, phase) -> None:
    tracker, bus, errors = tracked_bus
    await emit_valid(bus, errors, JOB_QUEUED, queued)
    before = await tracker.get_job(JOB_1)

    await emit_invalid(
        bus,
        errors,
        JOB_STARTED,
        JobStarted(JOB_1, phase, time(1)),
        InvalidJobTransition,
    )
    assert await tracker.get_job(JOB_1) == before


@pytest.mark.parametrize("phase", [JobPhase.PRODUCING, JobPhase.UPLOADING])
@pytest.mark.parametrize(
    "status",
    [JobStatus.UPLOADING, JobStatus.COMPLETED, JobStatus.FAILED],
)
async def test_job_started_rejects_uploading_and_terminal_jobs(
    tracked_bus, status, phase
) -> None:
    tracker, bus, errors = tracked_bus
    await create_job_at_status(bus, errors, status)
    before = await tracker.get_job(JOB_1)

    await emit_invalid(
        bus,
        errors,
        JOB_STARTED,
        JobStarted(JOB_1, phase, time(9)),
        InvalidJobTransition,
    )
    assert await tracker.get_job(JOB_1) == before


@pytest.mark.parametrize(
    ("staged", "phase"), [(False, JobPhase.PRODUCING), (True, JobPhase.UPLOADING)]
)
async def test_job_started_accepts_interrupted_job(tracked_bus, staged, phase) -> None:
    """Startup recovery flips a stuck job to INTERRUPTED, then re-queues it.

    The pump re-emits JOB_STARTED for that same job_id — it must not be
    rejected as an invalid transition, or recovery silently kills the
    scheduler via the error topic instead of retrying the job.
    """
    tracker, bus, errors = tracked_bus
    await emit_valid(bus, errors, JOB_QUEUED, staged_queued() if staged else youtube_queued())
    row = await tracker._jobs.get(JOB_1)
    row.status = JobStatus.INTERRUPTED
    await tracker._jobs.update(row)

    await emit_valid(bus, errors, JOB_STARTED, JobStarted(JOB_1, phase, time(1)))

    snapshot = await tracker.get_job(JOB_1)
    assert snapshot is not None
    assert snapshot.status is JobStatus(phase.value)
    assert snapshot.updated_at == time(1)


async def test_artifact_ready_rejects_waiting_job(tracked_bus) -> None:
    tracker, bus, errors = tracked_bus
    await emit_valid(bus, errors, JOB_QUEUED, youtube_queued())
    await emit_invalid(bus, errors, ARTIFACT_READY, ready(), InvalidJobTransition)
    assert (await tracker.get_job(JOB_1)).artifact_id is None  # type: ignore[union-attr]


@pytest.mark.parametrize("status", [JobStatus.COMPLETED, JobStatus.FAILED])
async def test_artifact_ready_rejects_terminal_jobs(tracked_bus, status) -> None:
    tracker, bus, errors = tracked_bus
    await create_job_at_status(bus, errors, status)
    before = await tracker.get_job(JOB_1)

    await emit_invalid(
        bus,
        errors,
        ARTIFACT_READY,
        ready(occurred_at=time(9)),
        InvalidJobTransition,
    )
    assert await tracker.get_job(JOB_1) == before


async def test_staged_artifact_ready_rejects_mismatched_artifact(tracked_bus) -> None:
    tracker, bus, errors = tracked_bus
    await emit_valid(bus, errors, JOB_QUEUED, staged_queued())
    await emit_valid(bus, errors, JOB_STARTED, JobStarted(JOB_1, JobPhase.UPLOADING, time(1)))
    await emit_invalid(
        bus,
        errors,
        ARTIFACT_READY,
        ready(artifact_id=ARTIFACT_2, occurred_at=time(2)),
    )

    assert (await tracker.get_job(JOB_1)).artifact_id == ARTIFACT_1  # type: ignore[union-attr]


async def test_production_failure_records_optional_artifact_and_error(tracked_bus) -> None:
    tracker, bus, errors = tracked_bus
    event = ArtifactProductionFailed(JOB_1, ARTIFACT_1, DOWNLOAD_ERROR, time(2))
    await emit_valid(bus, errors, JOB_QUEUED, youtube_queued())
    await emit_valid(bus, errors, JOB_STARTED, JobStarted(JOB_1, JobPhase.PRODUCING, time(1)))
    await emit_valid(bus, errors, ARTIFACT_PRODUCTION_FAILED, event)

    snapshot = await tracker.get_job(JOB_1)
    assert snapshot is not None
    assert snapshot.status is JobStatus.FAILED
    assert snapshot.artifact_id == ARTIFACT_1
    assert snapshot.error == DOWNLOAD_ERROR
    assert snapshot.updated_at == time(2)


async def test_upload_failure_records_error(tracked_bus) -> None:
    tracker, bus, errors = tracked_bus
    event = ArtifactUploadFailed(JOB_1, ARTIFACT_1, UPLOAD_ERROR, time(3))
    await emit_valid(bus, errors, JOB_QUEUED, youtube_queued())
    await emit_valid(bus, errors, JOB_STARTED, JobStarted(JOB_1, JobPhase.PRODUCING, time(1)))
    await emit_valid(bus, errors, ARTIFACT_READY, ready(occurred_at=time(2)))
    await emit_valid(bus, errors, ARTIFACT_UPLOAD_FAILED, event)

    snapshot = await tracker.get_job(JOB_1)
    assert snapshot is not None
    assert snapshot.status is JobStatus.FAILED
    assert snapshot.error == UPLOAD_ERROR
    assert snapshot.updated_at == time(3)


@pytest.mark.parametrize(
    "status",
    [
        JobStatus.WAITING,
        JobStatus.UPLOADING,
        JobStatus.COMPLETED,
        JobStatus.FAILED,
    ],
)
async def test_production_failure_rejects_every_status_except_producing(
    tracked_bus, status
) -> None:
    tracker, bus, errors = tracked_bus
    await create_job_at_status(bus, errors, status)
    before = await tracker.get_job(JOB_1)

    await emit_invalid(
        bus,
        errors,
        ARTIFACT_PRODUCTION_FAILED,
        ArtifactProductionFailed(JOB_1, None, DOWNLOAD_ERROR, time(9)),
        InvalidJobTransition,
    )
    assert await tracker.get_job(JOB_1) == before


@pytest.mark.parametrize(
    ("topic", "event"),
    [
        (ARTIFACT_UPLOADED, uploaded(occurred_at=time(9))),
        (
            ARTIFACT_UPLOAD_FAILED,
            ArtifactUploadFailed(JOB_1, ARTIFACT_1, UPLOAD_ERROR, time(9)),
        ),
    ],
)
@pytest.mark.parametrize(
    "status",
    [
        JobStatus.WAITING,
        JobStatus.PRODUCING,
        JobStatus.COMPLETED,
        JobStatus.FAILED,
    ],
)
async def test_upload_terminal_rejects_every_status_except_uploading(
    tracked_bus, status, topic, event
) -> None:
    tracker, bus, errors = tracked_bus
    await create_job_at_status(bus, errors, status)
    before = await tracker.get_job(JOB_1)

    await emit_invalid(bus, errors, topic, event, InvalidJobTransition)
    assert await tracker.get_job(JOB_1) == before


@pytest.mark.parametrize("terminal_kind", ["production_failed", "uploaded", "upload_failed"])
async def test_exact_duplicate_job_terminal_event_is_idempotent(tracked_bus, terminal_kind) -> None:
    tracker, bus, errors = tracked_bus
    await emit_valid(bus, errors, JOB_QUEUED, youtube_queued())
    await emit_valid(bus, errors, JOB_STARTED, JobStarted(JOB_1, JobPhase.PRODUCING, time(1)))
    if terminal_kind == "production_failed":
        topic = ARTIFACT_PRODUCTION_FAILED
        event = ArtifactProductionFailed(JOB_1, None, DOWNLOAD_ERROR, time(2))
    else:
        await emit_valid(bus, errors, ARTIFACT_READY, ready(occurred_at=time(2)))
        if terminal_kind == "uploaded":
            topic = ARTIFACT_UPLOADED
            event = uploaded(occurred_at=time(3))
        else:
            topic = ARTIFACT_UPLOAD_FAILED
            event = ArtifactUploadFailed(JOB_1, ARTIFACT_1, UPLOAD_ERROR, time(3))
    await emit_valid(bus, errors, topic, event)
    first = await tracker.get_job(JOB_1)

    await emit_valid(bus, errors, topic, event)
    assert await tracker.get_job(JOB_1) == first


@pytest.mark.parametrize("terminal_kind", ["production_failed", "uploaded", "upload_failed"])
async def test_changed_duplicate_job_terminal_event_is_invalid(tracked_bus, terminal_kind) -> None:
    tracker, bus, errors = tracked_bus
    await emit_valid(bus, errors, JOB_QUEUED, youtube_queued())
    await emit_valid(bus, errors, JOB_STARTED, JobStarted(JOB_1, JobPhase.PRODUCING, time(1)))
    if terminal_kind == "production_failed":
        topic = ARTIFACT_PRODUCTION_FAILED
        event = ArtifactProductionFailed(JOB_1, None, DOWNLOAD_ERROR, time(2))
    else:
        await emit_valid(bus, errors, ARTIFACT_READY, ready(occurred_at=time(2)))
        if terminal_kind == "uploaded":
            topic = ARTIFACT_UPLOADED
            event = uploaded(occurred_at=time(3))
        else:
            topic = ARTIFACT_UPLOAD_FAILED
            event = ArtifactUploadFailed(JOB_1, ARTIFACT_1, UPLOAD_ERROR, time(3))
    await emit_valid(bus, errors, topic, event)
    first = await tracker.get_job(JOB_1)

    await emit_invalid(bus, errors, topic, replace(event, occurred_at=time(9)), InvalidJobTransition)
    assert await tracker.get_job(JOB_1) == first


@pytest.mark.parametrize(
    ("first_kind", "second_kind"),
    [
        (first_kind, second_kind)
        for first_kind in ("production_failed", "uploaded", "upload_failed")
        for second_kind in ("production_failed", "uploaded", "upload_failed")
        if first_kind != second_kind
    ],
)
async def test_conflicting_job_terminal_event_is_invalid(
    tracked_bus, first_kind, second_kind
) -> None:
    tracker, bus, errors = tracked_bus
    await emit_valid(bus, errors, JOB_QUEUED, youtube_queued())
    await emit_valid(bus, errors, JOB_STARTED, JobStarted(JOB_1, JobPhase.PRODUCING, time(1)))
    if first_kind != "production_failed":
        await emit_valid(bus, errors, ARTIFACT_READY, ready(occurred_at=time(2)))
    first_topic, first_event = terminal_fact(first_kind, 3)
    second_topic, second_event = terminal_fact(second_kind, 4)
    await emit_valid(bus, errors, first_topic, first_event)
    first = await tracker.get_job(JOB_1)

    await emit_invalid(bus, errors, second_topic, second_event, InvalidJobTransition)
    assert await tracker.get_job(JOB_1) == first


@pytest.mark.parametrize(
    ("topic", "event"),
    [
        (ARTIFACT_UPLOADED, uploaded(artifact_id=ARTIFACT_2)),
        (
            ARTIFACT_UPLOAD_FAILED,
            ArtifactUploadFailed(JOB_1, ARTIFACT_2, UPLOAD_ERROR, NOW),
        ),
    ],
)
async def test_upload_terminal_rejects_mismatched_artifact(tracked_bus, topic, event) -> None:
    tracker, bus, errors = tracked_bus
    await emit_valid(bus, errors, JOB_QUEUED, youtube_queued())
    await emit_valid(bus, errors, JOB_STARTED, JobStarted(JOB_1, JobPhase.PRODUCING, time(1)))
    await emit_valid(bus, errors, ARTIFACT_READY, ready(occurred_at=time(2)))

    await emit_invalid(bus, errors, topic, event)
    assert (await tracker.get_job(JOB_1)).status is JobStatus.UPLOADING  # type: ignore[union-attr]


async def test_batch_created_has_exact_expanding_snapshot_and_zero_counts(tracked_bus) -> None:
    tracker, bus, errors = tracked_bus
    event = BatchCreated(BATCH, "https://example.test/playlist", NOW)
    await emit_valid(bus, errors, BATCH_CREATED, event)

    assert await tracker.get_batch(BATCH) == BatchSnapshot(
        id=BATCH,
        source_url="https://example.test/playlist",
        title=None,
        status=BatchStatus.EXPANDING,
        job_ids=(),
        skipped_entries=0,
        error=None,
        created_at=NOW,
        updated_at=NOW,
        total_jobs=0,
        waiting=0,
        producing=0,
        uploading=0,
        completed=0,
        failed=0,
    )


async def test_duplicate_batch_id_is_invalid_and_preserves_first_batch(tracked_bus) -> None:
    tracker, bus, errors = tracked_bus
    first = BatchCreated(BATCH, "https://example.test/first", NOW)
    await emit_valid(bus, errors, BATCH_CREATED, first)
    await emit_invalid(
        bus,
        errors,
        BATCH_CREATED,
        BatchCreated(BATCH, "https://example.test/secret", time(1)),
    )

    assert (await tracker.get_batch(BATCH)).source_url == first.source_url  # type: ignore[union-attr]


async def test_playlist_expanded_records_the_batch_title(tracked_bus) -> None:
    tracker, bus, errors = tracked_bus
    await emit_valid(
        bus,
        errors,
        BATCH_CREATED,
        BatchCreated(BATCH, "https://example.test/playlist", NOW),
    )
    await emit_valid(
        bus,
        errors,
        YOUTUBE_PLAYLIST_EXPANDED,
        PlaylistExpanded(BATCH, (), 0, time(1), "Rust Fundamentals"),
    )

    assert (await tracker.get_batch(BATCH)).title == "Rust Fundamentals"  # type: ignore[union-attr]


async def test_playlist_expanded_rejects_missing_batch(tracked_bus) -> None:
    _, bus, errors = tracked_bus
    await emit_invalid(
        bus,
        errors,
        YOUTUBE_PLAYLIST_EXPANDED,
        PlaylistExpanded(BATCH, (), 0, NOW, "Rust Fundamentals"),
    )


async def test_job_queued_with_a_title_is_recorded_on_the_snapshot(tracked_bus) -> None:
    tracker, bus, errors = tracked_bus
    await emit_valid(
        bus,
        errors,
        JOB_QUEUED,
        JobQueued(
            JOB_1,
            None,
            SourceKind.YOUTUBE,
            "https://example.test/watch?v=one",
            None,
            NOW,
            title="Episode One",
        ),
    )

    assert (await tracker.get_job(JOB_1)).title == "Episode One"  # type: ignore[union-attr]


@pytest.mark.parametrize(
    "event",
    [
        BatchJobsCreated(BATCH, (JOB_1,), 0, NOW),
        PlaylistExpansionFailed(BATCH, DOWNLOAD_ERROR, NOW),
    ],
)
async def test_batch_events_reject_missing_batch(tracked_bus, event) -> None:
    _, bus, errors = tracked_bus
    topic = (
        BATCH_JOBS_CREATED
        if isinstance(event, BatchJobsCreated)
        else YOUTUBE_PLAYLIST_EXPANSION_FAILED
    )
    await emit_invalid(bus, errors, topic, event)


@pytest.mark.parametrize(
    "job_ids",
    [(), (JOB_1, JOB_1)],
    ids=["empty", "duplicate"],
)
async def test_batch_attachment_requires_nonempty_unique_job_ids(tracked_bus, job_ids) -> None:
    tracker, bus, errors = tracked_bus
    await emit_valid(
        bus,
        errors,
        BATCH_CREATED,
        BatchCreated(BATCH, "https://example.test/playlist", NOW),
    )
    await emit_valid(bus, errors, JOB_QUEUED, youtube_queued(batch_id=BATCH))

    await emit_invalid(
        bus,
        errors,
        BATCH_JOBS_CREATED,
        BatchJobsCreated(BATCH, job_ids, 0, time(1)),
    )
    assert (await tracker.get_batch(BATCH)).status is BatchStatus.EXPANDING  # type: ignore[union-attr]


async def test_batch_attachment_requires_existing_jobs(tracked_bus) -> None:
    tracker, bus, errors = tracked_bus
    await emit_valid(
        bus,
        errors,
        BATCH_CREATED,
        BatchCreated(BATCH, "https://example.test/playlist", NOW),
    )
    await emit_invalid(
        bus,
        errors,
        BATCH_JOBS_CREATED,
        BatchJobsCreated(BATCH, (JOB_1,), 0, time(1)),
    )
    assert (await tracker.get_batch(BATCH)).job_ids == ()  # type: ignore[union-attr]


async def test_batch_attachment_requires_jobs_matching_batch(tracked_bus) -> None:
    tracker, bus, errors = tracked_bus
    await emit_valid(
        bus,
        errors,
        BATCH_CREATED,
        BatchCreated(BATCH, "https://example.test/playlist", NOW),
    )
    await emit_valid(bus, errors, JOB_QUEUED, youtube_queued(batch_id=None))

    await emit_invalid(
        bus,
        errors,
        BATCH_JOBS_CREATED,
        BatchJobsCreated(BATCH, (JOB_1,), 0, time(1)),
    )
    assert (await tracker.get_batch(BATCH)).job_ids == ()  # type: ignore[union-attr]


@pytest.mark.parametrize(
    "attached_ids",
    [(JOB_1,), (JOB_2, JOB_1), (JOB_1, JOB_2, JOB_3)],
    ids=["omission", "reordered", "extra"],
)
async def test_batch_attachment_must_exactly_match_pending_jobs_in_order(
    tracked_bus, attached_ids
) -> None:
    tracker, bus, errors = tracked_bus
    await emit_valid(
        bus,
        errors,
        BATCH_CREATED,
        BatchCreated(BATCH, "https://example.test/playlist", time(0)),
    )
    await emit_valid(
        bus,
        errors,
        JOB_QUEUED,
        youtube_queued(JOB_1, batch_id=BATCH, occurred_at=time(1)),
    )
    await emit_valid(
        bus,
        errors,
        JOB_QUEUED,
        youtube_queued(JOB_2, batch_id=BATCH, occurred_at=time(2)),
    )

    await emit_invalid(
        bus,
        errors,
        BATCH_JOBS_CREATED,
        BatchJobsCreated(BATCH, attached_ids, 0, time(3)),
    )
    assert (await tracker.get_batch(BATCH)).job_ids == ()  # type: ignore[union-attr]
    await emit_valid(
        bus,
        errors,
        BATCH_JOBS_CREATED,
        BatchJobsCreated(BATCH, (JOB_1, JOB_2), 0, time(4)),
    )


async def test_batched_job_cannot_be_queued_after_batch_attachment(tracked_bus) -> None:
    tracker, bus, errors = tracked_bus
    await create_batch_and_jobs(bus, errors)

    await emit_invalid(
        bus,
        errors,
        JOB_QUEUED,
        youtube_queued(JOB_3, batch_id=BATCH, occurred_at=time(4)),
    )
    assert await tracker.get_job(JOB_3) is None


async def test_batched_job_cannot_be_queued_after_expansion_failure(tracked_bus) -> None:
    tracker, bus, errors = tracked_bus
    await emit_valid(
        bus,
        errors,
        BATCH_CREATED,
        BatchCreated(BATCH, "https://example.test/playlist", time(0)),
    )
    await emit_valid(
        bus,
        errors,
        YOUTUBE_PLAYLIST_EXPANSION_FAILED,
        PlaylistExpansionFailed(BATCH, DOWNLOAD_ERROR, time(1)),
    )

    await emit_invalid(
        bus,
        errors,
        JOB_QUEUED,
        youtube_queued(JOB_1, batch_id=BATCH, occurred_at=time(2)),
    )
    assert await tracker.get_job(JOB_1) is None


async def test_expansion_failure_rejects_already_queued_pending_child(tracked_bus) -> None:
    tracker, bus, errors = tracked_bus
    await emit_valid(
        bus,
        errors,
        BATCH_CREATED,
        BatchCreated(BATCH, "https://example.test/playlist", time(0)),
    )
    await emit_valid(
        bus,
        errors,
        JOB_QUEUED,
        youtube_queued(JOB_1, batch_id=BATCH, occurred_at=time(1)),
    )

    await emit_invalid(
        bus,
        errors,
        YOUTUBE_PLAYLIST_EXPANSION_FAILED,
        PlaylistExpansionFailed(BATCH, DOWNLOAD_ERROR, time(2)),
    )
    assert (await tracker.get_batch(BATCH)).status is BatchStatus.EXPANDING  # type: ignore[union-attr]


async def test_batch_attachment_is_only_allowed_while_expanding(tracked_bus) -> None:
    tracker, bus, errors = tracked_bus
    await create_batch_and_jobs(bus, errors)
    original = await tracker.get_batch(BATCH)

    await emit_invalid(
        bus,
        errors,
        BATCH_JOBS_CREATED,
        BatchJobsCreated(BATCH, (JOB_1, JOB_2), 0, time(4)),
    )
    assert await tracker.get_batch(BATCH) == original


async def test_batch_attachment_preserves_order_skips_and_exact_counts(tracked_bus) -> None:
    tracker, bus, errors = tracked_bus
    await create_batch_and_jobs(bus, errors, (JOB_2, JOB_1), skipped_entries=3)

    snapshot = await tracker.get_batch(BATCH)
    assert snapshot is not None
    assert snapshot.status is BatchStatus.WAITING
    assert snapshot.job_ids == (JOB_2, JOB_1)
    assert snapshot.skipped_entries == 3
    assert snapshot.updated_at == time(3)
    assert (
        snapshot.total_jobs,
        snapshot.waiting,
        snapshot.producing,
        snapshot.uploading,
        snapshot.completed,
        snapshot.failed,
    ) == (2, 2, 0, 0, 0, 0)


async def test_playlist_expansion_failure_and_exact_duplicate_are_idempotent(tracked_bus) -> None:
    tracker, bus, errors = tracked_bus
    await emit_valid(
        bus,
        errors,
        BATCH_CREATED,
        BatchCreated(BATCH, "https://example.test/playlist", NOW),
    )
    event = PlaylistExpansionFailed(BATCH, DOWNLOAD_ERROR, time(1))
    await emit_valid(bus, errors, YOUTUBE_PLAYLIST_EXPANSION_FAILED, event)
    first = await tracker.get_batch(BATCH)
    await emit_valid(bus, errors, YOUTUBE_PLAYLIST_EXPANSION_FAILED, event)

    assert await tracker.get_batch(BATCH) == first
    assert first is not None
    assert first.status is BatchStatus.FAILED
    assert first.error == DOWNLOAD_ERROR
    assert first.updated_at == time(1)


async def test_conflicting_or_late_playlist_expansion_failure_is_invalid(tracked_bus) -> None:
    tracker, bus, errors = tracked_bus
    await emit_valid(
        bus,
        errors,
        BATCH_CREATED,
        BatchCreated(BATCH, "https://example.test/playlist", NOW),
    )
    event = PlaylistExpansionFailed(BATCH, DOWNLOAD_ERROR, time(1))
    await emit_valid(bus, errors, YOUTUBE_PLAYLIST_EXPANSION_FAILED, event)
    first = await tracker.get_batch(BATCH)
    await emit_invalid(
        bus,
        errors,
        YOUTUBE_PLAYLIST_EXPANSION_FAILED,
        replace(event, occurred_at=time(2)),
    )
    assert await tracker.get_batch(BATCH) == first


async def test_playlist_expansion_failure_rejects_attached_batch(tracked_bus) -> None:
    tracker, bus, errors = tracked_bus
    await create_batch_and_jobs(bus, errors)
    first = await tracker.get_batch(BATCH)

    await emit_invalid(
        bus,
        errors,
        YOUTUBE_PLAYLIST_EXPANSION_FAILED,
        PlaylistExpansionFailed(BATCH, DOWNLOAD_ERROR, time(4)),
    )
    assert await tracker.get_batch(BATCH) == first


@pytest.mark.parametrize(
    ("statuses", "skipped", "expected_status", "expected_counts"),
    [
        ((JobStatus.WAITING, JobStatus.WAITING), 0, BatchStatus.WAITING, (2, 0, 0, 0, 0)),
        ((JobStatus.WAITING, JobStatus.PRODUCING), 0, BatchStatus.PROCESSING, (1, 1, 0, 0, 0)),
        ((JobStatus.PRODUCING, JobStatus.PRODUCING), 0, BatchStatus.PROCESSING, (0, 2, 0, 0, 0)),
        ((JobStatus.PRODUCING, JobStatus.UPLOADING), 0, BatchStatus.PROCESSING, (0, 1, 1, 0, 0)),
        ((JobStatus.UPLOADING, JobStatus.WAITING), 0, BatchStatus.PROCESSING, (1, 0, 1, 0, 0)),
        ((JobStatus.UPLOADING, JobStatus.UPLOADING), 0, BatchStatus.PROCESSING, (0, 0, 2, 0, 0)),
        ((JobStatus.COMPLETED, JobStatus.WAITING), 0, BatchStatus.PROCESSING, (1, 0, 0, 1, 0)),
        ((JobStatus.COMPLETED, JobStatus.PRODUCING), 0, BatchStatus.PROCESSING, (0, 1, 0, 1, 0)),
        ((JobStatus.COMPLETED, JobStatus.UPLOADING), 0, BatchStatus.PROCESSING, (0, 0, 1, 1, 0)),
        ((JobStatus.FAILED, JobStatus.WAITING), 0, BatchStatus.PROCESSING, (1, 0, 0, 0, 1)),
        ((JobStatus.FAILED, JobStatus.PRODUCING), 0, BatchStatus.PROCESSING, (0, 1, 0, 0, 1)),
        ((JobStatus.FAILED, JobStatus.UPLOADING), 0, BatchStatus.PROCESSING, (0, 0, 1, 0, 1)),
        ((JobStatus.COMPLETED, JobStatus.COMPLETED), 0, BatchStatus.COMPLETED, (0, 0, 0, 2, 0)),
        (
            (JobStatus.COMPLETED, JobStatus.COMPLETED),
            2,
            BatchStatus.PARTIALLY_COMPLETED,
            (0, 0, 0, 2, 0),
        ),
        ((JobStatus.FAILED, JobStatus.FAILED), 3, BatchStatus.FAILED, (0, 0, 0, 0, 2)),
        (
            (JobStatus.COMPLETED, JobStatus.FAILED),
            0,
            BatchStatus.PARTIALLY_COMPLETED,
            (0, 0, 0, 1, 1),
        ),
    ],
)
async def test_batch_status_and_counts_are_derived_from_current_children(
    tracked_bus, statuses, skipped, expected_status, expected_counts
) -> None:
    tracker, bus, errors = tracked_bus
    await create_batch_and_jobs(bus, errors, skipped_entries=skipped)
    step = 4
    for job_id, status in zip((JOB_1, JOB_2), statuses, strict=True):
        step = await move_job(bus, errors, job_id, status, step)

    snapshot = await tracker.get_batch(BATCH)
    assert snapshot is not None
    assert snapshot.status is expected_status
    assert snapshot.total_jobs == 2
    assert (
        snapshot.waiting,
        snapshot.producing,
        snapshot.uploading,
        snapshot.completed,
        snapshot.failed,
    ) == expected_counts


async def test_batch_updated_at_advances_for_each_attached_child_event(tracked_bus) -> None:
    tracker, bus, errors = tracked_bus
    await create_batch_and_jobs(bus, errors)
    assert (await tracker.get_batch(BATCH)).updated_at == time(3)  # type: ignore[union-attr]

    await emit_valid(bus, errors, JOB_STARTED, JobStarted(JOB_1, JobPhase.PRODUCING, time(4)))
    assert (await tracker.get_batch(BATCH)).updated_at == time(4)  # type: ignore[union-attr]
    await emit_valid(bus, errors, ARTIFACT_READY, ready(occurred_at=time(5)))
    assert (await tracker.get_batch(BATCH)).updated_at == time(5)  # type: ignore[union-attr]
    await emit_valid(bus, errors, ARTIFACT_UPLOADED, uploaded(occurred_at=time(6)))
    assert (await tracker.get_batch(BATCH)).updated_at == time(6)  # type: ignore[union-attr]


async def test_batch_freshness_never_regresses_across_children(tracked_bus) -> None:
    tracker, bus, errors = tracked_bus
    await create_batch_and_jobs(bus, errors)
    await emit_valid(
        bus,
        errors,
        JOB_STARTED,
        JobStarted(JOB_1, JobPhase.PRODUCING, time(6)),
    )
    await emit_valid(
        bus,
        errors,
        JOB_STARTED,
        JobStarted(JOB_2, JobPhase.PRODUCING, time(5)),
    )

    assert (await tracker.get_job(JOB_2)).updated_at == time(5)  # type: ignore[union-attr]
    assert (await tracker.get_batch(BATCH)).updated_at == time(6)  # type: ignore[union-attr]


async def test_returned_snapshots_are_frozen_and_do_not_expose_internal_mutation(tracked_bus) -> None:
    tracker, bus, errors = tracked_bus
    await create_batch_and_jobs(bus, errors)
    job = await tracker.get_job(JOB_1)
    batch = await tracker.get_batch(BATCH)
    assert job is not None and batch is not None

    with pytest.raises(FrozenInstanceError):
        job.status = JobStatus.FAILED  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        batch.job_ids = ()  # type: ignore[misc]
    assert isinstance(batch.job_ids, tuple)

    await emit_valid(bus, errors, JOB_STARTED, JobStarted(JOB_1, JobPhase.PRODUCING, time(4)))
    assert job.status is JobStatus.WAITING
    assert batch.waiting == 2
    assert (await tracker.get_job(JOB_1)).status is JobStatus.PRODUCING  # type: ignore[union-attr]
    assert (await tracker.get_batch(BATCH)).waiting == 1  # type: ignore[union-attr]


async def test_list_queue_is_empty_when_nothing_is_tracked(tracked_bus) -> None:
    tracker, _, _ = tracked_bus
    assert await tracker.list_queue() == ()


async def test_list_queue_returns_an_expanding_batch_with_no_children(tracked_bus) -> None:
    tracker, bus, errors = tracked_bus
    await emit_valid(
        bus,
        errors,
        BATCH_CREATED,
        BatchCreated(BATCH, "https://example.test/playlist", NOW),
    )

    entries = await tracker.list_queue()

    assert len(entries) == 1
    entry = entries[0]
    assert isinstance(entry, BatchEntry)
    assert entry.batch.status is BatchStatus.EXPANDING
    assert entry.jobs == ()


async def test_list_queue_groups_batch_children_in_the_batchs_own_order(tracked_bus) -> None:
    tracker, bus, errors = tracked_bus
    await create_batch_and_jobs(bus, errors, (JOB_2, JOB_1))

    entries = await tracker.list_queue()

    assert len(entries) == 1
    entry = entries[0]
    assert isinstance(entry, BatchEntry)
    assert [job.id for job in entry.jobs] == [JOB_2, JOB_1]


async def test_list_queue_excludes_batched_jobs_from_the_standalone_list(tracked_bus) -> None:
    tracker, bus, errors = tracked_bus
    await create_batch_and_jobs(bus, errors)

    entries = await tracker.list_queue()

    assert len(entries) == 1
    assert isinstance(entries[0], BatchEntry)


async def test_list_queue_interleaves_standalone_jobs_and_batches_newest_first(
    tracked_bus,
) -> None:
    tracker, bus, errors = tracked_bus
    batch_2 = UUID("20000000-0000-0000-0000-000000000002")
    await emit_valid(bus, errors, JOB_QUEUED, youtube_queued(JOB_1, occurred_at=time(0)))
    await emit_valid(
        bus,
        errors,
        BATCH_CREATED,
        BatchCreated(BATCH, "https://example.test/first", time(1)),
    )
    await emit_valid(bus, errors, JOB_QUEUED, youtube_queued(JOB_2, occurred_at=time(2)))
    await emit_valid(
        bus,
        errors,
        BATCH_CREATED,
        BatchCreated(batch_2, "https://example.test/second", time(3)),
    )

    entries = await tracker.list_queue()

    kinds = [
        ("batch", entry.batch.id) if isinstance(entry, BatchEntry) else ("job", entry.id)
        for entry in entries
    ]
    assert kinds == [
        ("batch", batch_2),
        ("job", JOB_2),
        ("batch", BATCH),
        ("job", JOB_1),
    ]


@pytest.mark.parametrize("register_tracker_first", [True, False])
async def test_real_pyee_routes_async_listener_errors_without_registration_order_assumption(
    register_tracker_first: bool, tmp_path: Path
) -> None:
    tracker, conn = await _build_tracker(tmp_path)
    bus = AsyncIOEventEmitter()
    errors: list[Exception] = []
    if register_tracker_first:
        tracker.register(bus)
        bus.on("error", errors.append)
    else:
        bus.on("error", errors.append)
        tracker.register(bus)

    assert bus.emit(JOB_STARTED, JobStarted(JOB_1, JobPhase.PRODUCING, NOW))
    await _settle(bus)
    assert len(errors) == 1
    assert isinstance(errors[0], TrackingError)
    assert await tracker.get_job(JOB_1) is None
    await conn.close()
