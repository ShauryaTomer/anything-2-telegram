from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from uuid import UUID

from .domain import ErrorInfo, JobPhase, SourceKind, StagedArtifact


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


@dataclass(frozen=True)
class BatchCreated:
    batch_id: UUID
    source_url: str
    occurred_at: datetime


@dataclass(frozen=True)
class JobQueued:
    job_id: UUID
    batch_id: UUID | None
    source_kind: SourceKind
    source: str
    staged_artifact: StagedArtifact | None
    occurred_at: datetime


@dataclass(frozen=True)
class JobStarted:
    job_id: UUID
    phase: JobPhase
    occurred_at: datetime


@dataclass(frozen=True)
class BatchJobsCreated:
    batch_id: UUID
    job_ids: tuple[UUID, ...]
    skipped_entries: int
    occurred_at: datetime


@dataclass(frozen=True)
class PlaylistExpansionRequested:
    batch_id: UUID
    source_url: str
    occurred_at: datetime
    offset: int = 0


@dataclass(frozen=True)
class DownloadTarget:
    source_id: str
    source_url: str
    caption_prefix: str = ""


@dataclass(frozen=True)
class PlaylistExpanded:
    batch_id: UUID
    targets: tuple[DownloadTarget, ...]
    skipped_entries: int
    occurred_at: datetime
    playlist_title: str | None = None


@dataclass(frozen=True)
class PlaylistExpansionFailed:
    batch_id: UUID
    error: ErrorInfo
    occurred_at: datetime


@dataclass(frozen=True)
class YouTubeDownloadRequested:
    job_id: UUID
    source_url: str
    occurred_at: datetime
    caption_prefix: str = ""


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


@dataclass(frozen=True)
class ArtifactProductionFailed:
    job_id: UUID
    artifact_id: UUID | None
    error: ErrorInfo
    occurred_at: datetime


@dataclass(frozen=True)
class ArtifactUploaded:
    job_id: UUID
    artifact_id: UUID
    telegram_chat_id: int
    telegram_message_id: int
    occurred_at: datetime


@dataclass(frozen=True)
class ArtifactUploadFailed:
    job_id: UUID
    artifact_id: UUID
    error: ErrorInfo
    occurred_at: datetime


@dataclass(frozen=True)
class TelegramUnavailable:
    error: ErrorInfo
    occurred_at: datetime
