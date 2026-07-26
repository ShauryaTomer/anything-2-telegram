from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from uuid import UUID


class SourceKind(str, Enum):
    YOUTUBE = "youtube"
    LOCAL_UPLOAD = "local_upload"


class JobStatus(str, Enum):
    WAITING = "waiting"
    PRODUCING = "producing"
    UPLOADING = "uploading"
    COMPLETED = "completed"
    FAILED = "failed"


class BatchStatus(str, Enum):
    EXPANDING = "expanding"
    WAITING = "waiting"
    PROCESSING = "processing"
    COMPLETED = "completed"
    PARTIALLY_COMPLETED = "partially_completed"
    FAILED = "failed"


class JobPhase(str, Enum):
    PRODUCING = "producing"
    UPLOADING = "uploading"


@dataclass(frozen=True)
class ErrorInfo:
    code: str
    message: str


@dataclass(frozen=True)
class StagedArtifact:
    job_id: UUID
    artifact_id: UUID
    local_path: Path
    filename: str
    media_type: str | None
    size_bytes: int
    caption: str | None


@dataclass(frozen=True)
class JobSnapshot:
    id: UUID
    batch_id: UUID | None
    source_kind: SourceKind
    source: str
    status: JobStatus
    artifact_id: UUID | None
    filename: str | None
    size_bytes: int | None
    telegram_message_id: int | None
    error: ErrorInfo | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class BatchSnapshot:
    id: UUID
    source_url: str
    status: BatchStatus
    job_ids: tuple[UUID, ...]
    skipped_entries: int
    error: ErrorInfo | None
    created_at: datetime
    updated_at: datetime
    total_jobs: int
    waiting: int
    producing: int
    uploading: int
    completed: int
    failed: int


@dataclass(frozen=True)
class JobRef:
    id: UUID
    status_url: str


@dataclass(frozen=True)
class BatchRef:
    id: UUID
    status_url: str


@dataclass(frozen=True)
class UploadReservation:
    job_id: UUID
    artifact_id: UUID
    destination: Path
    filename: str
    media_type: str | None
    caption: str | None


@dataclass(frozen=True)
class ProcessResult:
    exit_code: int
    stdout: str
    stderr_safe_summary: str


@dataclass(frozen=True)
class TelegramUploadResult:
    chat_id: int
    message_id: int
