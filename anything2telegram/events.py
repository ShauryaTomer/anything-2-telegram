from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from uuid import UUID

from .domain import (
    ErrorInfo,
    JobPhase,
    SourceKind,
    StagedArtifact,
    _require_nonblank,
    _require_nonnegative,
    _require_optional_uuid,
    _require_type,
    _require_uuid_tuple,
)


BATCH_CREATED = "batch.created"
BATCH_JOBS_CREATED = "batch.jobs.created"
JOB_QUEUED = "job.queued"
JOB_STARTED = "job.started"
YOUTUBE_PLAYLIST_EXPANSION_REQUESTED = "youtube.playlist.expansion.requested"
YOUTUBE_DOWNLOAD_REQUESTED = "youtube.download.requested"
YOUTUBE_PLAYLIST_EXPANDED = "youtube.playlist.expanded"
YOUTUBE_PLAYLIST_EXPANSION_FAILED = "youtube.playlist.expansion.failed"
ARTIFACT_READY = "artifact.ready"
ARTIFACT_PRODUCTION_FAILED = "artifact.production.failed"
ARTIFACT_UPLOADED = "artifact.uploaded"
ARTIFACT_UPLOAD_FAILED = "artifact.upload.failed"
TELEGRAM_UNAVAILABLE = "telegram.unavailable"
ERROR = "error"


def _require_event_time(occurred_at: object) -> None:
    _require_type(occurred_at, datetime, "occurred_at")


@dataclass(frozen=True)
class BatchCreated:
    batch_id: UUID
    source_url: str
    occurred_at: datetime

    def __post_init__(self) -> None:
        _require_type(self.batch_id, UUID, "batch_id")
        _require_event_time(self.occurred_at)
        _require_nonblank(self.source_url, "source_url")


@dataclass(frozen=True)
class JobQueued:
    job_id: UUID
    batch_id: UUID | None
    source_kind: SourceKind
    source: str
    staged_artifact: StagedArtifact | None
    occurred_at: datetime

    def __post_init__(self) -> None:
        _require_type(self.job_id, UUID, "job_id")
        _require_optional_uuid(self.batch_id, "batch_id")
        _require_type(self.source_kind, SourceKind, "source_kind")
        if self.staged_artifact is not None:
            _require_type(self.staged_artifact, StagedArtifact, "staged_artifact")
        _require_event_time(self.occurred_at)
        _require_nonblank(self.source, "source")
        if self.source_kind is SourceKind.YOUTUBE:
            if self.staged_artifact is not None:
                raise ValueError("youtube jobs cannot have a staged artifact")
            return
        if self.source_kind is SourceKind.LOCAL_UPLOAD:
            if self.batch_id is not None:
                raise ValueError("local upload jobs cannot belong to a batch")
            if self.staged_artifact is None:
                raise ValueError("local upload jobs require a staged artifact")
            if self.staged_artifact.job_id != self.job_id:
                raise ValueError("staged artifact job_id must match job_id")


@dataclass(frozen=True)
class JobStarted:
    job_id: UUID
    phase: JobPhase
    occurred_at: datetime

    def __post_init__(self) -> None:
        _require_type(self.job_id, UUID, "job_id")
        _require_type(self.phase, JobPhase, "phase")
        _require_event_time(self.occurred_at)


@dataclass(frozen=True)
class BatchJobsCreated:
    batch_id: UUID
    job_ids: tuple[UUID, ...]
    skipped_entries: int
    occurred_at: datetime

    def __post_init__(self) -> None:
        _require_type(self.batch_id, UUID, "batch_id")
        _require_uuid_tuple(self.job_ids, "job_ids")
        _require_event_time(self.occurred_at)
        _require_nonnegative(self.skipped_entries, "skipped_entries")


@dataclass(frozen=True)
class PlaylistExpansionRequested:
    batch_id: UUID
    source_url: str
    occurred_at: datetime

    def __post_init__(self) -> None:
        _require_type(self.batch_id, UUID, "batch_id")
        _require_event_time(self.occurred_at)
        _require_nonblank(self.source_url, "source_url")


@dataclass(frozen=True)
class DownloadTarget:
    source_id: str
    source_url: str

    def __post_init__(self) -> None:
        _require_nonblank(self.source_id, "source_id")
        _require_nonblank(self.source_url, "source_url")


@dataclass(frozen=True)
class PlaylistExpanded:
    batch_id: UUID
    targets: tuple[DownloadTarget, ...]
    skipped_entries: int
    occurred_at: datetime

    def __post_init__(self) -> None:
        _require_type(self.batch_id, UUID, "batch_id")
        _require_type(self.targets, tuple, "targets")
        if not all(isinstance(target, DownloadTarget) for target in self.targets):
            raise TypeError("targets must contain only DownloadTarget values")
        source_ids = [target.source_id for target in self.targets]
        if len(source_ids) != len(set(source_ids)):
            raise ValueError("targets must have unique source_id values")
        _require_event_time(self.occurred_at)
        _require_nonnegative(self.skipped_entries, "skipped_entries")


@dataclass(frozen=True)
class PlaylistExpansionFailed:
    batch_id: UUID
    error: ErrorInfo
    occurred_at: datetime

    def __post_init__(self) -> None:
        _require_type(self.batch_id, UUID, "batch_id")
        _require_event_time(self.occurred_at)


@dataclass(frozen=True)
class YouTubeDownloadRequested:
    job_id: UUID
    source_url: str
    occurred_at: datetime

    def __post_init__(self) -> None:
        _require_type(self.job_id, UUID, "job_id")
        _require_event_time(self.occurred_at)
        _require_nonblank(self.source_url, "source_url")


@dataclass(frozen=True)
class ArtifactReady:
    job_id: UUID
    artifact_id: UUID
    local_path: Path
    filename: str
    media_type: str | None
    size_bytes: int
    caption: str | None
    occurred_at: datetime

    def __post_init__(self) -> None:
        _require_type(self.job_id, UUID, "job_id")
        _require_type(self.artifact_id, UUID, "artifact_id")
        _require_event_time(self.occurred_at)
        _require_nonblank(self.filename, "filename")
        _require_nonnegative(self.size_bytes, "size_bytes")


@dataclass(frozen=True)
class ArtifactProductionFailed:
    job_id: UUID
    artifact_id: UUID | None
    error: ErrorInfo
    occurred_at: datetime

    def __post_init__(self) -> None:
        _require_type(self.job_id, UUID, "job_id")
        _require_optional_uuid(self.artifact_id, "artifact_id")
        _require_event_time(self.occurred_at)


@dataclass(frozen=True)
class ArtifactUploaded:
    job_id: UUID
    artifact_id: UUID
    telegram_chat_id: int
    telegram_message_id: int
    occurred_at: datetime

    def __post_init__(self) -> None:
        _require_type(self.job_id, UUID, "job_id")
        _require_type(self.artifact_id, UUID, "artifact_id")
        _require_event_time(self.occurred_at)


@dataclass(frozen=True)
class ArtifactUploadFailed:
    job_id: UUID
    artifact_id: UUID
    error: ErrorInfo
    occurred_at: datetime

    def __post_init__(self) -> None:
        _require_type(self.job_id, UUID, "job_id")
        _require_type(self.artifact_id, UUID, "artifact_id")
        _require_event_time(self.occurred_at)


@dataclass(frozen=True)
class TelegramUnavailable:
    error: ErrorInfo
    occurred_at: datetime

    def __post_init__(self) -> None:
        _require_event_time(self.occurred_at)
