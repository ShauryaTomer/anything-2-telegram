from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest

from anything2telegram.domain import (
    BatchRef,
    BatchSnapshot,
    BatchStatus,
    ErrorInfo,
    JobPhase,
    JobRef,
    JobSnapshot,
    JobStatus,
    ProcessResult,
    SourceKind,
    StagedArtifact,
    TelegramUploadResult,
    UploadReservation,
)
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


NOW = datetime(2026, 7, 19, 8, 30, tzinfo=UTC)


def test_enum_values_are_stable_strings() -> None:
    assert [item.value for item in SourceKind] == ["youtube", "local_upload"]
    assert [item.value for item in JobStatus] == [
        "waiting",
        "producing",
        "uploading",
        "completed",
        "failed",
    ]
    assert [item.value for item in BatchStatus] == [
        "expanding",
        "waiting",
        "processing",
        "completed",
        "partially_completed",
        "failed",
    ]
    assert [item.value for item in JobPhase] == ["producing", "uploading"]
    assert isinstance(SourceKind.YOUTUBE, str)


def test_topic_values_are_exact() -> None:
    assert {
        BATCH_CREATED,
        BATCH_JOBS_CREATED,
        JOB_QUEUED,
        JOB_STARTED,
        YOUTUBE_PLAYLIST_EXPANSION_REQUESTED,
        YOUTUBE_DOWNLOAD_REQUESTED,
        YOUTUBE_PLAYLIST_EXPANDED,
        YOUTUBE_PLAYLIST_EXPANSION_FAILED,
        ARTIFACT_READY,
        ARTIFACT_PRODUCTION_FAILED,
        ARTIFACT_UPLOADED,
        ARTIFACT_UPLOAD_FAILED,
        TELEGRAM_UNAVAILABLE,
        ERROR,
    } == {
        "batch.created",
        "batch.jobs.created",
        "job.queued",
        "job.started",
        "youtube.playlist.expansion.requested",
        "youtube.download.requested",
        "youtube.playlist.expanded",
        "youtube.playlist.expansion.failed",
        "artifact.ready",
        "artifact.production.failed",
        "artifact.uploaded",
        "artifact.upload.failed",
        "telegram.unavailable",
        "error",
    }


def _domain_values() -> list[object]:
    job_id = uuid4()
    batch_id = uuid4()
    artifact_id = uuid4()
    error = ErrorInfo("download_failed", "Download failed")
    return [
        error,
        StagedArtifact(
            job_id,
            artifact_id,
            Path("/tmp/video.mp4"),
            "video.mp4",
            "video/mp4",
            12,
            "caption",
        ),
        JobSnapshot(
            job_id,
            batch_id,
            SourceKind.YOUTUBE,
            "https://example.test/watch?v=1",
            JobStatus.WAITING,
            artifact_id,
            "video.mp4",
            12,
            99,
            None,
            NOW,
            NOW,
        ),
        BatchSnapshot(
            batch_id,
            "https://example.test/playlist",
            BatchStatus.WAITING,
            (job_id,),
            1,
            None,
            NOW,
            NOW,
        ),
        JobRef(job_id, f"/jobs/{job_id}"),
        BatchRef(batch_id, f"/batches/{batch_id}"),
        UploadReservation(
            job_id,
            artifact_id,
            Path("/tmp/video.mp4"),
            "video.mp4",
            "video/mp4",
            "caption",
        ),
        ProcessResult(0, "output", ""),
        TelegramUploadResult(-100123, 7),
    ]


def _event_values() -> list[object]:
    job_id = uuid4()
    batch_id = uuid4()
    artifact_id = uuid4()
    error = ErrorInfo("telegram_unavailable", "Telegram unavailable")
    staged = StagedArtifact(
        job_id,
        artifact_id,
        Path("/tmp/video.mp4"),
        "video.mp4",
        "video/mp4",
        12,
        None,
    )
    target = DownloadTarget("video-id", "https://example.test/watch?v=video-id")
    return [
        BatchCreated(batch_id, "https://example.test/playlist", NOW),
        JobQueued(job_id, None, SourceKind.LOCAL_UPLOAD, "video.mp4", staged, NOW),
        JobStarted(job_id, JobPhase.PRODUCING, NOW),
        BatchJobsCreated(batch_id, (job_id,), 1, NOW),
        PlaylistExpansionRequested(batch_id, "https://example.test/playlist", NOW),
        target,
        PlaylistExpanded(batch_id, (target,), 1, NOW),
        PlaylistExpansionFailed(batch_id, error, NOW),
        YouTubeDownloadRequested(job_id, "https://example.test/watch?v=1", NOW),
        ArtifactReady(
            job_id,
            artifact_id,
            Path("/tmp/video.mp4"),
            "video.mp4",
            "video/mp4",
            12,
            None,
            NOW,
        ),
        ArtifactProductionFailed(job_id, artifact_id, error, NOW),
        ArtifactUploaded(job_id, artifact_id, -100123, 7, NOW),
        ArtifactUploadFailed(job_id, artifact_id, error, NOW),
        TelegramUnavailable(error, NOW),
    ]


@pytest.mark.parametrize("value", _domain_values() + _event_values())
def test_value_objects_are_frozen(value: object) -> None:
    field_name = next(iter(value.__dataclass_fields__))  # type: ignore[attr-defined]
    with pytest.raises(FrozenInstanceError):
        setattr(value, field_name, None)


@pytest.mark.parametrize(
    "event",
    [value for value in _event_values() if not isinstance(value, DownloadTarget)],
)
def test_every_emitted_payload_keeps_its_timestamp(event: object) -> None:
    assert event.occurred_at is NOW  # type: ignore[attr-defined]


def test_payloads_keep_scoped_ids_and_runtime_types() -> None:
    batch_id = uuid4()
    job_id = uuid4()
    target = DownloadTarget("id", "https://example.test/video")
    event = PlaylistExpanded(batch_id, (target,), 0, NOW)

    assert event.batch_id == batch_id
    assert event.targets == (target,)
    assert isinstance(event.targets, tuple)
    assert isinstance(event.targets[0], DownloadTarget)

    started = JobStarted(job_id, JobPhase.UPLOADING, NOW)
    assert started.job_id == job_id
    assert started.phase is JobPhase.UPLOADING


def _staged(job_id=None, size_bytes: int = 1) -> StagedArtifact:
    resolved_job_id = job_id or uuid4()
    return StagedArtifact(
        resolved_job_id,
        uuid4(),
        Path("/tmp/upload.mp4"),
        "upload.mp4",
        "video/mp4",
        size_bytes,
        None,
    )


def test_youtube_job_rejects_a_staged_artifact() -> None:
    job_id = uuid4()
    with pytest.raises(ValueError, match="youtube"):
        JobQueued(
            job_id,
            uuid4(),
            SourceKind.YOUTUBE,
            "https://example.test/video",
            _staged(job_id),
            NOW,
        )


def test_local_upload_requires_no_batch_and_a_matching_staged_artifact() -> None:
    job_id = uuid4()
    with pytest.raises(ValueError, match="batch"):
        JobQueued(
            job_id,
            uuid4(),
            SourceKind.LOCAL_UPLOAD,
            "upload.mp4",
            _staged(job_id),
            NOW,
        )
    with pytest.raises(ValueError, match="staged"):
        JobQueued(job_id, None, SourceKind.LOCAL_UPLOAD, "upload.mp4", None, NOW)
    with pytest.raises(ValueError, match="job_id"):
        JobQueued(
            job_id,
            None,
            SourceKind.LOCAL_UPLOAD,
            "upload.mp4",
            _staged(),
            NOW,
        )


@pytest.mark.parametrize(
    "factory",
    [
        lambda: StagedArtifact(
            uuid4(), uuid4(), Path("/tmp/x"), "x", None, -1, None
        ),
        lambda: ArtifactReady(
            uuid4(), uuid4(), Path("/tmp/x"), "x", None, -1, None, NOW
        ),
        lambda: BatchJobsCreated(uuid4(), (), -1, NOW),
        lambda: PlaylistExpanded(uuid4(), (), -1, NOW),
        lambda: BatchSnapshot(
            uuid4(), "url", BatchStatus.WAITING, (), -1, None, NOW, NOW
        ),
        lambda: JobSnapshot(
            uuid4(),
            None,
            SourceKind.YOUTUBE,
            "url",
            JobStatus.WAITING,
            None,
            None,
            -1,
            None,
            None,
            NOW,
            NOW,
        ),
    ],
)
def test_negative_counts_and_sizes_are_rejected(factory) -> None:
    with pytest.raises(ValueError):
        factory()


@pytest.mark.parametrize(
    "factory",
    [
        lambda: ErrorInfo(" ", "message"),
        lambda: ErrorInfo("code", " "),
        lambda: BatchCreated(uuid4(), " ", NOW),
        lambda: PlaylistExpansionRequested(uuid4(), "", NOW),
        lambda: YouTubeDownloadRequested(uuid4(), "\t", NOW),
        lambda: DownloadTarget("", "url"),
        lambda: DownloadTarget("id", " "),
        lambda: JobQueued(uuid4(), uuid4(), SourceKind.YOUTUBE, "", None, NOW),
        lambda: StagedArtifact(
            uuid4(), uuid4(), Path("/tmp/x"), "", None, 1, None
        ),
        lambda: ArtifactReady(
            uuid4(), uuid4(), Path("/tmp/x"), " ", None, 1, None, NOW
        ),
        lambda: JobRef(uuid4(), ""),
        lambda: BatchRef(uuid4(), " "),
        lambda: UploadReservation(
            uuid4(), uuid4(), Path("/tmp/x"), "", None, None
        ),
    ],
)
def test_blank_required_strings_are_rejected(factory) -> None:
    with pytest.raises(ValueError):
        factory()

