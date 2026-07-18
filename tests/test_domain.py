from dataclasses import FrozenInstanceError, fields, replace
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

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


def _event_cases() -> list[tuple[object, dict[str, object]]]:
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
        (
            BatchCreated(batch_id, "https://example.test/playlist", NOW),
            {"batch_id": batch_id},
        ),
        (
            JobQueued(
                job_id,
                batch_id,
                SourceKind.YOUTUBE,
                "https://example.test/watch?v=1",
                None,
                NOW,
            ),
            {"job_id": job_id, "batch_id": batch_id},
        ),
        (
            JobQueued(
                job_id, None, SourceKind.LOCAL_UPLOAD, "video.mp4", staged, NOW
            ),
            {"job_id": job_id, "batch_id": None},
        ),
        (JobStarted(job_id, JobPhase.PRODUCING, NOW), {"job_id": job_id}),
        (
            BatchJobsCreated(batch_id, (job_id,), 1, NOW),
            {"batch_id": batch_id, "job_ids": (job_id,)},
        ),
        (
            PlaylistExpansionRequested(
                batch_id, "https://example.test/playlist", NOW
            ),
            {"batch_id": batch_id},
        ),
        (target, {}),
        (
            PlaylistExpanded(batch_id, (target,), 1, NOW),
            {"batch_id": batch_id},
        ),
        (
            PlaylistExpansionFailed(batch_id, error, NOW),
            {"batch_id": batch_id},
        ),
        (
            YouTubeDownloadRequested(
                job_id, "https://example.test/watch?v=1", NOW
            ),
            {"job_id": job_id},
        ),
        (
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
            {"job_id": job_id, "artifact_id": artifact_id},
        ),
        (
            ArtifactProductionFailed(job_id, artifact_id, error, NOW),
            {"job_id": job_id, "artifact_id": artifact_id},
        ),
        (
            ArtifactUploaded(job_id, artifact_id, -100123, 7, NOW),
            {"job_id": job_id, "artifact_id": artifact_id},
        ),
        (
            ArtifactUploadFailed(job_id, artifact_id, error, NOW),
            {"job_id": job_id, "artifact_id": artifact_id},
        ),
        (TelegramUnavailable(error, NOW), {}),
    ]


def _event_values() -> list[object]:
    return [event for event, _ in _event_cases()]


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


def test_batch_snapshot_exposes_job_ids_in_order() -> None:
    job_ids = (uuid4(), uuid4())
    snapshot = BatchSnapshot(
        uuid4(),
        "https://example.test/playlist",
        BatchStatus.WAITING,
        job_ids,
        0,
        None,
        NOW,
        NOW,
    )

    assert snapshot.job_ids == job_ids
    assert not hasattr(snapshot, "ordered_job_ids")


@pytest.mark.parametrize(
    ("event", "expected_ids"),
    [case for case in _event_cases() if not isinstance(case[0], DownloadTarget)],
)
def test_every_emitted_payload_keeps_exact_scoped_ids(
    event: object, expected_ids: dict[str, object]
) -> None:
    assert {
        field_name: getattr(event, field_name) for field_name in expected_ids
    } == expected_ids


@pytest.mark.parametrize(
    "event",
    [value for value in _event_values() if not isinstance(value, DownloadTarget)],
)
def test_every_emitted_payload_has_typed_scoped_ids_and_timestamp(event: object) -> None:
    assert isinstance(event.occurred_at, datetime)  # type: ignore[attr-defined]
    for field in fields(event):
        value = getattr(event, field.name)
        if field.name in {"job_id", "batch_id", "artifact_id"} and value is not None:
            assert isinstance(value, UUID)
        if field.name == "job_ids":
            assert isinstance(value, tuple)
            assert all(isinstance(item, UUID) for item in value)


@pytest.mark.parametrize(
    "event",
    [value for value in _event_values() if not isinstance(value, DownloadTarget)],
)
def test_emitted_payloads_reject_non_datetime_timestamps(event: object) -> None:
    with pytest.raises(TypeError, match="occurred_at"):
        replace(event, occurred_at="2026-07-19")


def _event_uuid_fields() -> list[tuple[object, str]]:
    cases: list[tuple[object, str]] = []
    for event in _event_values():
        if isinstance(event, DownloadTarget):
            continue
        for field in fields(event):
            value = getattr(event, field.name)
            if field.name in {"job_id", "batch_id", "artifact_id"} and value is not None:
                cases.append((event, field.name))
    return cases


@pytest.mark.parametrize(("event", "field_name"), _event_uuid_fields())
def test_emitted_payloads_reject_non_uuid_scoped_ids(
    event: object, field_name: str
) -> None:
    with pytest.raises(TypeError, match=field_name):
        replace(event, **{field_name: "not-a-uuid"})


def test_batch_jobs_created_rejects_non_uuid_job_ids() -> None:
    with pytest.raises(TypeError, match="job_ids"):
        BatchJobsCreated(uuid4(), (uuid4(), "not-a-uuid"), 0, NOW)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "factory",
    [
        lambda: JobSnapshot(
            uuid4(),
            None,
            "youtube",
            "url",
            JobStatus.WAITING,
            None,
            None,
            None,
            None,
            None,
            NOW,
            NOW,
        ),
        lambda: JobSnapshot(
            uuid4(),
            None,
            SourceKind.YOUTUBE,
            "url",
            "waiting",
            None,
            None,
            None,
            None,
            None,
            NOW,
            NOW,
        ),
        lambda: BatchSnapshot(
            uuid4(), "url", "waiting", (), 0, None, NOW, NOW
        ),
        lambda: JobQueued(uuid4(), None, "local_upload", "upload", _staged(), NOW),
        lambda: JobStarted(uuid4(), "producing", NOW),
    ],
)
def test_enum_typed_fields_reject_plain_strings(factory) -> None:
    with pytest.raises(TypeError):
        factory()


def test_playlist_expanded_rejects_duplicate_source_ids() -> None:
    targets = (
        DownloadTarget("same-id", "https://example.test/one"),
        DownloadTarget("same-id", "https://example.test/two"),
    )

    with pytest.raises(ValueError, match="source_id"):
        PlaylistExpanded(uuid4(), targets, 0, NOW)


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
