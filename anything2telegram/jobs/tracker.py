from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from ..domain import (
    BatchEntry,
    BatchSnapshot,
    BatchStatus,
    ErrorInfo,
    JobPhase,
    JobSnapshot,
    JobStatus,
    SourceKind,
)
from ..events import (
    ARTIFACT_PRODUCTION_FAILED,
    ARTIFACT_READY,
    ARTIFACT_UPLOAD_FAILED,
    ARTIFACT_UPLOADED,
    BATCH_CREATED,
    BATCH_JOBS_CREATED,
    JOB_QUEUED,
    JOB_STARTED,
    YOUTUBE_PLAYLIST_EXPANSION_FAILED,
    ArtifactProductionFailed,
    ArtifactReady,
    ArtifactUploadFailed,
    ArtifactUploaded,
    BatchCreated,
    BatchJobsCreated,
    JobQueued,
    JobStarted,
    PlaylistExpansionFailed,
)


class TrackingError(RuntimeError):
    """Lifecycle fact cannot be applied to the in-memory projection."""


class InvalidJobTransition(TrackingError):
    """Job lifecycle fact conflicts with its current state."""


@dataclass
class _JobRecord:
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
    staged: bool
    ready_event: ArtifactReady | None = None
    terminal_event: object | None = None


@dataclass
class _BatchRecord:
    id: UUID
    source_url: str
    job_ids: tuple[UUID, ...]
    skipped_entries: int
    error: ErrorInfo | None
    created_at: datetime
    updated_at: datetime
    pending_job_ids: tuple[UUID, ...] = ()
    terminal_event: object | None = None


class JobTracker:
    def __init__(self) -> None:
        self._jobs: dict[UUID, _JobRecord] = {}
        self._batches: dict[UUID, _BatchRecord] = {}

    def register(self, bus) -> None:
        bus.on(BATCH_CREATED, self._on_batch_created)
        bus.on(BATCH_JOBS_CREATED, self._on_batch_jobs_created)
        bus.on(JOB_QUEUED, self._on_job_queued)
        bus.on(JOB_STARTED, self._on_job_started)
        bus.on(ARTIFACT_READY, self._on_artifact_ready)
        bus.on(ARTIFACT_PRODUCTION_FAILED, self._on_artifact_production_failed)
        bus.on(ARTIFACT_UPLOADED, self._on_artifact_uploaded)
        bus.on(ARTIFACT_UPLOAD_FAILED, self._on_artifact_upload_failed)
        bus.on(
            YOUTUBE_PLAYLIST_EXPANSION_FAILED,
            self._on_playlist_expansion_failed,
        )

    def get_job(self, job_id: UUID) -> JobSnapshot | None:
        record = self._jobs.get(job_id)
        if record is None:
            return None
        return JobSnapshot(
            id=record.id,
            batch_id=record.batch_id,
            source_kind=record.source_kind,
            source=record.source,
            status=record.status,
            artifact_id=record.artifact_id,
            filename=record.filename,
            size_bytes=record.size_bytes,
            telegram_message_id=record.telegram_message_id,
            error=record.error,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )

    def get_batch(self, batch_id: UUID) -> BatchSnapshot | None:
        record = self._batches.get(batch_id)
        if record is None:
            return None
        counts = {
            status: sum(
                self._jobs[job_id].status is status for job_id in record.job_ids
            )
            for status in JobStatus
        }
        status = self._derive_batch_status(record, counts)
        return BatchSnapshot(
            id=record.id,
            source_url=record.source_url,
            status=status,
            job_ids=record.job_ids,
            skipped_entries=record.skipped_entries,
            error=record.error,
            created_at=record.created_at,
            updated_at=record.updated_at,
            total_jobs=len(record.job_ids),
            waiting=counts[JobStatus.WAITING],
            producing=counts[JobStatus.PRODUCING],
            uploading=counts[JobStatus.UPLOADING],
            completed=counts[JobStatus.COMPLETED],
            failed=counts[JobStatus.FAILED],
        )

    def list_queue(self) -> tuple[JobSnapshot | BatchEntry, ...]:
        entries: list[tuple[datetime, JobSnapshot | BatchEntry]] = []
        for job_id, job_record in self._jobs.items():
            if job_record.batch_id is None:
                snapshot = self.get_job(job_id)
                entries.append((snapshot.created_at, snapshot))
        for batch_id, batch_record in self._batches.items():
            batch_snapshot = self.get_batch(batch_id)
            children = tuple(self.get_job(job_id) for job_id in batch_record.job_ids)
            entries.append((batch_snapshot.created_at, BatchEntry(batch_snapshot, children)))
        entries.sort(key=lambda entry: entry[0], reverse=True)
        return tuple(entry for _, entry in entries)

    def _on_batch_created(self, event: BatchCreated) -> None:
        if event.batch_id in self._batches:
            raise TrackingError("duplicate batch")
        self._batches[event.batch_id] = _BatchRecord(
            id=event.batch_id,
            source_url=event.source_url,
            job_ids=(),
            skipped_entries=0,
            error=None,
            created_at=event.occurred_at,
            updated_at=event.occurred_at,
        )

    def _on_job_queued(self, event: JobQueued) -> None:
        if event.job_id in self._jobs:
            raise TrackingError("duplicate job")
        batch = None
        if event.batch_id is not None:
            batch = self._require_batch(event.batch_id)
            if batch.job_ids or batch.terminal_event is not None:
                raise TrackingError("invalid batch transition")
        staged = event.staged_artifact
        self._jobs[event.job_id] = _JobRecord(
            id=event.job_id,
            batch_id=event.batch_id,
            source_kind=event.source_kind,
            source=event.source,
            status=JobStatus.WAITING,
            artifact_id=staged.artifact_id if staged is not None else None,
            filename=staged.filename if staged is not None else None,
            size_bytes=staged.size_bytes if staged is not None else None,
            telegram_message_id=None,
            error=None,
            created_at=event.occurred_at,
            updated_at=event.occurred_at,
            staged=staged is not None,
        )
        if batch is not None:
            batch.pending_job_ids += (event.job_id,)
        self._touch_batch(self._jobs[event.job_id], event.occurred_at)

    def _on_job_started(self, event: JobStarted) -> None:
        record = self._require_job(event.job_id)
        self._require_status(record, JobStatus.WAITING)
        expected_phase = JobPhase.UPLOADING if record.staged else JobPhase.PRODUCING
        if event.phase is not expected_phase:
            raise InvalidJobTransition("invalid job phase")
        record.status = JobStatus(event.phase.value)
        self._advance(record, event.occurred_at)

    def _on_artifact_ready(self, event: ArtifactReady) -> None:
        record = self._require_job(event.job_id)
        if record.ready_event is not None:
            if record.ready_event == event:
                return
            raise InvalidJobTransition("conflicting artifact ready event")
        if record.staged:
            self._require_artifact(record, event.artifact_id)
        if record.status is JobStatus.UPLOADING:
            if not record.staged:
                raise InvalidJobTransition("invalid job transition")
        elif record.status is not JobStatus.PRODUCING:
            raise InvalidJobTransition("invalid job transition")
        record.status = JobStatus.UPLOADING
        record.artifact_id = event.artifact_id
        record.filename = event.filename
        record.size_bytes = event.size_bytes
        record.ready_event = event
        self._advance(record, event.occurred_at)

    def _on_artifact_production_failed(
        self, event: ArtifactProductionFailed
    ) -> None:
        record = self._require_job(event.job_id)
        if self._terminal_duplicate(record, event):
            return
        self._require_status(record, JobStatus.PRODUCING)
        if event.artifact_id is not None:
            if record.artifact_id is not None:
                self._require_artifact(record, event.artifact_id)
            record.artifact_id = event.artifact_id
        record.status = JobStatus.FAILED
        record.error = event.error
        record.terminal_event = event
        self._advance(record, event.occurred_at)

    def _on_artifact_uploaded(self, event: ArtifactUploaded) -> None:
        record = self._require_job(event.job_id)
        if self._terminal_duplicate(record, event):
            return
        self._require_status(record, JobStatus.UPLOADING)
        self._require_artifact(record, event.artifact_id)
        record.status = JobStatus.COMPLETED
        record.telegram_message_id = event.telegram_message_id
        record.terminal_event = event
        self._advance(record, event.occurred_at)

    def _on_artifact_upload_failed(self, event: ArtifactUploadFailed) -> None:
        record = self._require_job(event.job_id)
        if self._terminal_duplicate(record, event):
            return
        self._require_status(record, JobStatus.UPLOADING)
        self._require_artifact(record, event.artifact_id)
        record.status = JobStatus.FAILED
        record.error = event.error
        record.terminal_event = event
        self._advance(record, event.occurred_at)

    def _on_batch_jobs_created(self, event: BatchJobsCreated) -> None:
        record = self._require_batch(event.batch_id)
        if record.job_ids or record.terminal_event is not None:
            raise TrackingError("invalid batch transition")
        if not record.pending_job_ids or event.job_ids != record.pending_job_ids:
            raise TrackingError("batch jobs do not match queued jobs")
        for job_id in event.job_ids:
            job = self._jobs.get(job_id)
            if job is None:
                raise TrackingError("unknown job")
            if job.batch_id != event.batch_id:
                raise TrackingError("job does not belong to batch")
        record.job_ids = event.job_ids
        record.pending_job_ids = ()
        record.skipped_entries = event.skipped_entries
        record.updated_at = event.occurred_at

    def _on_playlist_expansion_failed(
        self, event: PlaylistExpansionFailed
    ) -> None:
        record = self._require_batch(event.batch_id)
        if record.terminal_event is not None:
            if record.terminal_event == event:
                return
            raise TrackingError("conflicting batch terminal event")
        if record.job_ids or record.pending_job_ids:
            raise TrackingError("invalid batch transition")
        record.error = event.error
        record.terminal_event = event
        record.updated_at = event.occurred_at

    def _require_job(self, job_id: UUID) -> _JobRecord:
        try:
            return self._jobs[job_id]
        except KeyError:
            raise TrackingError("unknown job") from None

    def _require_batch(self, batch_id: UUID) -> _BatchRecord:
        try:
            return self._batches[batch_id]
        except KeyError:
            raise TrackingError("unknown batch") from None

    @staticmethod
    def _require_status(record: _JobRecord, expected: JobStatus) -> None:
        if record.status is not expected:
            raise InvalidJobTransition("invalid job transition")

    @staticmethod
    def _require_artifact(record: _JobRecord, artifact_id: UUID) -> None:
        if record.artifact_id != artifact_id:
            raise TrackingError("artifact does not match job")

    @staticmethod
    def _terminal_duplicate(record: _JobRecord, event: object) -> bool:
        if record.terminal_event is None:
            return False
        if record.terminal_event == event:
            return True
        raise InvalidJobTransition("conflicting job terminal event")

    def _advance(self, record: _JobRecord, occurred_at: datetime) -> None:
        record.updated_at = occurred_at
        self._touch_batch(record, occurred_at)

    def _touch_batch(self, job: _JobRecord, occurred_at: datetime) -> None:
        if job.batch_id is None:
            return
        batch = self._batches[job.batch_id]
        batch.updated_at = max(batch.updated_at, occurred_at)

    @staticmethod
    def _derive_batch_status(
        record: _BatchRecord, counts: dict[JobStatus, int]
    ) -> BatchStatus:
        if record.terminal_event is not None:
            return BatchStatus.FAILED
        if not record.job_ids:
            return BatchStatus.EXPANDING
        nonterminal = (
            counts[JobStatus.WAITING]
            + counts[JobStatus.PRODUCING]
            + counts[JobStatus.UPLOADING]
        )
        if nonterminal:
            if counts[JobStatus.WAITING] == len(record.job_ids):
                return BatchStatus.WAITING
            return BatchStatus.PROCESSING
        if counts[JobStatus.FAILED] == len(record.job_ids):
            return BatchStatus.FAILED
        if counts[JobStatus.COMPLETED] == len(record.job_ids):
            if record.skipped_entries:
                return BatchStatus.PARTIALLY_COMPLETED
            return BatchStatus.COMPLETED
        return BatchStatus.PARTIALLY_COMPLETED
