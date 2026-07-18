from dataclasses import FrozenInstanceError, fields, replace
from datetime import UTC, datetime, timedelta, timezone, tzinfo
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
            1,
            1,
            0,
            0,
            0,
            0,
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
        2,
        2,
        0,
        0,
        0,
        0,
    )

    assert snapshot.job_ids == job_ids
    assert not hasattr(snapshot, "ordered_job_ids")


def test_batch_snapshot_total_jobs_must_match_job_ids() -> None:
    snapshot = _valid_domain_value(BatchSnapshot)

    with pytest.raises(ValueError, match="total_jobs"):
        replace(snapshot, total_jobs=2)


def test_batch_snapshot_status_counts_must_sum_to_total_jobs() -> None:
    snapshot = _valid_domain_value(BatchSnapshot)

    with pytest.raises(ValueError, match="counts"):
        replace(snapshot, waiting=0)


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


@pytest.mark.parametrize(
    "event",
    [value for value in _event_values() if not isinstance(value, DownloadTarget)],
    ids=lambda event: type(event).__name__,
)
def test_every_emitted_payload_rejects_naive_timestamp(event: object) -> None:
    with pytest.raises(ValueError, match="occurred_at must be timezone-aware"):
        replace(event, occurred_at=NOW.replace(tzinfo=None))


class _InvalidOffset(tzinfo):
    def utcoffset(self, dt: datetime | None) -> timedelta:
        return timedelta(hours=24)


def test_event_timestamp_rejects_invalid_utc_offset_with_stable_error() -> None:
    invalid_time = datetime(2026, 7, 19, 8, 30, tzinfo=_InvalidOffset())

    with pytest.raises(ValueError, match="occurred_at must be timezone-aware"):
        BatchCreated(uuid4(), "https://example.test/playlist", invalid_time)


def test_event_timestamp_accepts_aware_non_utc_offset() -> None:
    occurred_at = datetime(
        2026,
        7,
        19,
        14,
        0,
        tzinfo=timezone(timedelta(hours=5, minutes=30)),
    )

    assert BatchCreated(
        uuid4(), "https://example.test/playlist", occurred_at
    ).occurred_at is occurred_at


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
            uuid4(),
            "url",
            BatchStatus.WAITING,
            (),
            -1,
            None,
            NOW,
            NOW,
            0,
            0,
            0,
            0,
            0,
            0,
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


def _valid_domain_value(value_type: type) -> object:
    return next(value for value in _domain_values() if isinstance(value, value_type))


@pytest.mark.parametrize(
    ("value", "changes"),
    [
        (ErrorInfo("code", "message"), {"code": 1}),
        (_valid_domain_value(StagedArtifact), {"local_path": "/tmp/video.mp4"}),
        (_valid_domain_value(JobSnapshot), {"error": "failure"}),
        (_valid_domain_value(BatchSnapshot), {"error": "failure"}),
        (_valid_domain_value(JobRef), {"id": "not-a-uuid"}),
        (_valid_domain_value(BatchRef), {"status_url": 42}),
        (_valid_domain_value(UploadReservation), {"caption": 42}),
        (ProcessResult(0, "stdout", "stderr"), {"returncode": True}),
        (TelegramUploadResult(-100, 1), {"message_id": 1.5}),
    ],
)
def test_every_domain_class_rejects_a_malformed_field(
    value: object, changes: dict[str, object]
) -> None:
    with pytest.raises(TypeError):
        replace(value, **changes)


@pytest.mark.parametrize(
    ("value", "changes"),
    [
        (_valid_domain_value(StagedArtifact), {"filename": 7}),
        (_valid_domain_value(StagedArtifact), {"media_type": 7}),
        (_valid_domain_value(StagedArtifact), {"size_bytes": True}),
        (_valid_domain_value(StagedArtifact), {"size_bytes": 1.5}),
        (_valid_domain_value(StagedArtifact), {"caption": 7}),
        (_valid_domain_value(JobSnapshot), {"filename": " "}),
        (_valid_domain_value(JobSnapshot), {"filename": 7}),
        (_valid_domain_value(JobSnapshot), {"size_bytes": True}),
        (_valid_domain_value(JobSnapshot), {"telegram_message_id": 1.5}),
        (_valid_domain_value(BatchSnapshot), {"skipped_entries": True}),
        (_valid_domain_value(UploadReservation), {"destination": "/tmp/video"}),
        (_valid_domain_value(UploadReservation), {"media_type": 7}),
        (ProcessResult(0, "stdout", "stderr"), {"stdout": 7}),
        (ProcessResult(0, "stdout", "stderr"), {"stderr_safe_summary": 7}),
        (TelegramUploadResult(-100, 1), {"chat_id": True}),
    ],
)
def test_domain_primitive_fields_enforce_exact_runtime_types(
    value: object, changes: dict[str, object]
) -> None:
    expected_error = ValueError if changes.get("filename") == " " else TypeError
    with pytest.raises(expected_error):
        replace(value, **changes)


def _event_of(value_type: type) -> object:
    return next(value for value in _event_values() if isinstance(value, value_type))


@pytest.mark.parametrize(
    ("event", "changes"),
    [
        (_event_of(BatchCreated), {"source_url": 7}),
        (_event_of(JobQueued), {"source": 7}),
        (_event_of(JobStarted), {"phase": "producing"}),
        (_event_of(BatchJobsCreated), {"skipped_entries": True}),
        (_event_of(PlaylistExpansionRequested), {"source_url": 7}),
        (DownloadTarget("id", "url"), {"source_id": 7}),
        (_event_of(PlaylistExpanded), {"targets": []}),
        (_event_of(PlaylistExpansionFailed), {"error": "failure"}),
        (_event_of(YouTubeDownloadRequested), {"source_url": 7}),
        (_event_of(ArtifactReady), {"local_path": "/tmp/video"}),
        (_event_of(ArtifactProductionFailed), {"error": "failure"}),
        (_event_of(ArtifactUploaded), {"telegram_chat_id": True}),
        (_event_of(ArtifactUploadFailed), {"error": "failure"}),
        (_event_of(TelegramUnavailable), {"error": "failure"}),
    ],
)
def test_every_event_class_rejects_a_malformed_field(
    event: object, changes: dict[str, object]
) -> None:
    with pytest.raises(TypeError):
        replace(event, **changes)


@pytest.mark.parametrize(
    ("changes", "expected_error"),
    [
        ({"filename": 7}, TypeError),
        ({"media_type": 7}, TypeError),
        ({"size_bytes": True}, TypeError),
        ({"size_bytes": 1.5}, TypeError),
        ({"caption": 7}, TypeError),
        ({"filename": " "}, ValueError),
    ],
)
def test_artifact_ready_enforces_all_artifact_field_contracts(
    changes: dict[str, object], expected_error: type[Exception]
) -> None:
    with pytest.raises(expected_error):
        replace(_event_of(ArtifactReady), **changes)


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    [
        ("telegram_chat_id", 1.5),
        ("telegram_message_id", True),
        ("telegram_message_id", 1.5),
    ],
)
def test_artifact_uploaded_requires_strict_integer_telegram_ids(
    field_name: str, invalid_value: object
) -> None:
    with pytest.raises(TypeError):
        replace(_event_of(ArtifactUploaded), **{field_name: invalid_value})
