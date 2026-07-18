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


def _require_nonblank(value: str, field_name: str) -> None:
    if not value.strip():
        raise ValueError(f"{field_name} must not be blank")


def _require_nonnegative(value: int, field_name: str) -> None:
    if value < 0:
        raise ValueError(f"{field_name} must not be negative")


def _require_type(value: object, expected_type: type, field_name: str) -> None:
    if not isinstance(value, expected_type):
        raise TypeError(f"{field_name} must be {expected_type.__name__}")


def _require_optional_uuid(value: object, field_name: str) -> None:
    if value is not None:
        _require_type(value, UUID, field_name)


def _require_uuid_tuple(value: object, field_name: str) -> None:
    _require_type(value, tuple, field_name)
    if not all(isinstance(item, UUID) for item in value):
        raise TypeError(f"{field_name} must contain only UUID values")


@dataclass(frozen=True)
class ErrorInfo:
    code: str
    message: str

    def __post_init__(self) -> None:
        _require_nonblank(self.code, "code")
        _require_nonblank(self.message, "message")


@dataclass(frozen=True)
class StagedArtifact:
    job_id: UUID
    artifact_id: UUID
    local_path: Path
    filename: str
    media_type: str | None
    size_bytes: int
    caption: str | None

    def __post_init__(self) -> None:
        _require_type(self.job_id, UUID, "job_id")
        _require_type(self.artifact_id, UUID, "artifact_id")
        _require_nonblank(self.filename, "filename")
        _require_nonnegative(self.size_bytes, "size_bytes")


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

    def __post_init__(self) -> None:
        _require_type(self.id, UUID, "id")
        _require_optional_uuid(self.batch_id, "batch_id")
        _require_type(self.source_kind, SourceKind, "source_kind")
        _require_type(self.status, JobStatus, "status")
        _require_optional_uuid(self.artifact_id, "artifact_id")
        _require_type(self.created_at, datetime, "created_at")
        _require_type(self.updated_at, datetime, "updated_at")
        _require_nonblank(self.source, "source")
        if self.size_bytes is not None:
            _require_nonnegative(self.size_bytes, "size_bytes")


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

    def __post_init__(self) -> None:
        _require_type(self.id, UUID, "id")
        _require_type(self.status, BatchStatus, "status")
        _require_uuid_tuple(self.job_ids, "job_ids")
        _require_type(self.created_at, datetime, "created_at")
        _require_type(self.updated_at, datetime, "updated_at")
        _require_nonblank(self.source_url, "source_url")
        _require_nonnegative(self.skipped_entries, "skipped_entries")


@dataclass(frozen=True)
class JobRef:
    id: UUID
    status_url: str

    def __post_init__(self) -> None:
        _require_type(self.id, UUID, "id")
        _require_nonblank(self.status_url, "status_url")


@dataclass(frozen=True)
class BatchRef:
    id: UUID
    status_url: str

    def __post_init__(self) -> None:
        _require_type(self.id, UUID, "id")
        _require_nonblank(self.status_url, "status_url")


@dataclass(frozen=True)
class UploadReservation:
    job_id: UUID
    artifact_id: UUID
    destination: Path
    filename: str
    media_type: str | None
    caption: str | None

    def __post_init__(self) -> None:
        _require_type(self.job_id, UUID, "job_id")
        _require_type(self.artifact_id, UUID, "artifact_id")
        _require_nonblank(self.filename, "filename")


@dataclass(frozen=True)
class ProcessResult:
    returncode: int
    stdout: str
    stderr_safe_summary: str


@dataclass(frozen=True)
class TelegramUploadResult:
    chat_id: int
    message_id: int
