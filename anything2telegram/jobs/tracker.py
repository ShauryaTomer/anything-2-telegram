import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from uuid import UUID

from ..domain import (
    BatchEntry,
    BatchSnapshot,
    BatchStatus,
    JobSnapshot,
    JobStatus,
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
    YOUTUBE_PLAYLIST_EXPANDED,
    YOUTUBE_PLAYLIST_EXPANSION_FAILED,
    BatchCreated,
    BatchJobsCreated,
    JobQueued,
    PlaylistExpanded,
    PlaylistExpansionFailed,
)
from . import lifecycle
from .lifecycle import (
    InvalidJobTransition as InvalidJobTransition,
    TrackingError as TrackingError,
)
from .repositories import BatchesRepository, BatchRow, JobsRepository, JobRow


class JobTracker:
    def __init__(self, jobs: JobsRepository, batches: BatchesRepository) -> None:
        self._jobs = jobs
        self._batches = batches
        # Handlers are scheduled tasks now, not inline sync calls, so two
        # causally-dependent facts for the same job/batch (e.g. JOB_STARTED
        # then ARTIFACT_READY, emitted back-to-back) could otherwise have
        # their read-mutate-write cycles interleave and race. The app is
        # already single-process/one-job-at-a-time, so serializing every
        # handler behind one lock just restores that same guarantee.
        self._lock = asyncio.Lock()

    def register(self, bus) -> None:
        bus.on(BATCH_CREATED, self._locked(self._on_batch_created))
        bus.on(BATCH_JOBS_CREATED, self._locked(self._on_batch_jobs_created))
        bus.on(JOB_QUEUED, self._locked(self._on_job_queued))
        bus.on(JOB_STARTED, self._locked(self._on_job_fact))
        bus.on(ARTIFACT_READY, self._locked(self._on_job_fact))
        bus.on(ARTIFACT_PRODUCTION_FAILED, self._locked(self._on_job_fact))
        bus.on(ARTIFACT_UPLOADED, self._locked(self._on_job_fact))
        bus.on(ARTIFACT_UPLOAD_FAILED, self._locked(self._on_job_fact))
        bus.on(YOUTUBE_PLAYLIST_EXPANDED, self._locked(self._on_playlist_expanded))
        bus.on(
            YOUTUBE_PLAYLIST_EXPANSION_FAILED,
            self._locked(self._on_playlist_expansion_failed),
        )

    def _locked(
        self, handler: Callable[[object], Awaitable[None]]
    ) -> Callable[[object], Awaitable[None]]:
        async def wrapped(event: object) -> None:
            async with self._lock:
                await handler(event)

        return wrapped

    async def get_job(self, job_id: UUID) -> JobSnapshot | None:
        row = await self._jobs.get(job_id)
        return None if row is None else _snapshot_from_row(row)

    async def get_batch(self, batch_id: UUID) -> BatchSnapshot | None:
        record = await self._batches.get(batch_id)
        if record is None:
            return None
        return await self._batch_snapshot(record)

    async def _batch_snapshot(self, record: BatchRow) -> BatchSnapshot:
        job_rows = await self._jobs.list_by_batch(record.id) if record.jobs_created else ()
        counts = {
            status: sum(row.status is status for row in job_rows) for status in JobStatus
        }
        job_ids = tuple(row.id for row in job_rows)
        status = self._derive_batch_status(record, job_ids, counts)
        return BatchSnapshot(
            id=record.id,
            source_url=record.source_url,
            title=record.title,
            status=status,
            job_ids=job_ids,
            skipped_entries=record.skipped_entries,
            error=record.error,
            created_at=record.created_at,
            updated_at=record.updated_at,
            total_jobs=len(job_ids),
            waiting=counts[JobStatus.WAITING],
            producing=counts[JobStatus.PRODUCING],
            uploading=counts[JobStatus.UPLOADING],
            completed=counts[JobStatus.COMPLETED],
            failed=counts[JobStatus.FAILED],
            thumbnail_url=record.thumbnail_url,
        )

    async def list_queue(self) -> tuple[JobSnapshot | BatchEntry, ...]:
        entries: list[tuple[datetime, JobSnapshot | BatchEntry]] = []
        for row in await self._jobs.list_standalone():
            snapshot = _snapshot_from_row(row)
            entries.append((snapshot.created_at, snapshot))
        for record in await self._batches.list_all():
            batch_snapshot = await self._batch_snapshot(record)
            job_rows = await self._jobs.list_by_batch(record.id)
            children = tuple(_snapshot_from_row(row) for row in job_rows)
            entries.append((batch_snapshot.created_at, BatchEntry(batch_snapshot, children)))
        entries.sort(key=lambda entry: entry[0], reverse=True)
        return tuple(entry for _, entry in entries)

    async def claim_failed_batch_jobs_for_retry(
        self, batch_id: UUID
    ) -> tuple[JobSnapshot, ...]:
        """Mark failed jobs WAITING so they can be retried, then return snapshots."""
        async with self._lock:
            await self._require_batch(batch_id)
            rows = await self._jobs.list_by_batch(batch_id)
            now = datetime.now(UTC)
            claimed_jobs: list[JobSnapshot] = []
            for row in rows:
                if row.status is not JobStatus.FAILED:
                    continue
                row.status = JobStatus.WAITING
                row.updated_at = now
                await self._jobs.update(row)
                claimed_jobs.append(_snapshot_from_row(row))

            if claimed_jobs:
                batch = await self._batches.get(batch_id)
                if batch is not None:
                    await self._touch_batch(batch, now)
            return tuple(claimed_jobs)

    async def claim_failed_job_for_retry(self, job_id: UUID) -> JobSnapshot | None:
        """Mark one failed job WAITING so it can be retried."""
        async with self._lock:
            row = await self._require_job(job_id)
            if row.status is not JobStatus.FAILED:
                return None
            now = datetime.now(UTC)
            row.status = JobStatus.WAITING
            row.updated_at = now
            await self._jobs.update(row)
            await self._touch_batch_for_job(row, now)
            return _snapshot_from_row(row)

    async def revert_batch_jobs_to_failed(
        self, jobs: tuple[JobSnapshot, ...], occurred_at: datetime | None = None
    ) -> None:
        """Undo a failed-batch retry claim by restoring WAITING jobs to FAILED."""
        if not jobs:
            return
        now = occurred_at if occurred_at is not None else datetime.now(UTC)
        async with self._lock:
            for snapshot in jobs:
                row = await self._jobs.get(snapshot.id)
                if row is None:
                    continue
                if row.status is not JobStatus.WAITING:
                    continue
                row.status = JobStatus.FAILED
                row.error = snapshot.error
                row.updated_at = now
                await self._jobs.update(row)
                batch = await self._batches.get(snapshot.batch_id) if snapshot.batch_id else None
                if batch is not None:
                    await self._touch_batch(batch, now)

    async def clear_retried_batch_job_state(
        self, jobs: tuple[JobSnapshot, ...], occurred_at: datetime | None = None
    ) -> None:
        """Clear stale artifact/chat state for jobs now requeued for retry."""
        if not jobs:
            return
        now = occurred_at if occurred_at is not None else datetime.now(UTC)
        async with self._lock:
            for snapshot in jobs:
                row = await self._jobs.get(snapshot.id)
                if row is None:
                    continue
                row.error = None
                row.telegram_chat_id = None
                row.telegram_message_id = None
                row.artifact_id = None
                row.filename = None
                row.size_bytes = None
                row.ready_local_path = None
                row.ready_media_type = None
                row.ready_caption = None
                row.updated_at = now
                await self._jobs.update(row)

    async def _on_batch_created(self, event: BatchCreated) -> None:
        if await self._batches.get(event.batch_id) is not None:
            raise TrackingError("duplicate batch")
        await self._batches.insert(
            BatchRow(
                id=event.batch_id,
                source_url=event.source_url,
                title=None,
                jobs_created=False,
                skipped_entries=0,
                error=None,
                created_at=event.occurred_at,
                updated_at=event.occurred_at,
            )
        )

    async def _on_job_queued(self, event: JobQueued) -> None:
        if await self._jobs.get(event.job_id) is not None:
            raise TrackingError("duplicate job")
        batch = None
        if event.batch_id is not None:
            batch = await self._require_batch(event.batch_id)
            if batch.jobs_created or batch.error is not None:
                raise TrackingError("invalid batch transition")
        staged = event.staged_artifact
        row = JobRow(
            id=event.job_id,
            batch_id=event.batch_id,
            source_kind=event.source_kind,
            source=event.source,
            title=event.title,
            status=JobStatus.WAITING,
            artifact_id=staged.artifact_id if staged is not None else None,
            filename=staged.filename if staged is not None else None,
            size_bytes=staged.size_bytes if staged is not None else None,
            telegram_chat_id=None,
            telegram_message_id=None,
            error=None,
            created_at=event.occurred_at,
            updated_at=event.occurred_at,
            staged=staged is not None,
        )
        await self._jobs.insert(row)
        if batch is not None:
            await self._touch_batch(batch, event.occurred_at)

    async def _on_job_fact(self, event: lifecycle.JobFact) -> None:
        record = await self._require_job(event.job_id)
        if lifecycle.apply(record, event):
            await self._jobs.update(record)
            await self._touch_batch_for_job(record, event.occurred_at)

    async def _on_batch_jobs_created(self, event: BatchJobsCreated) -> None:
        record = await self._require_batch(event.batch_id)
        if record.jobs_created or record.error is not None:
            raise TrackingError("invalid batch transition")
        pending = tuple(row.id for row in await self._jobs.list_by_batch(event.batch_id))
        if not pending or event.job_ids != pending:
            raise TrackingError("batch jobs do not match queued jobs")
        record.jobs_created = True
        record.skipped_entries = event.skipped_entries
        record.updated_at = event.occurred_at
        await self._batches.update(record)

    async def _on_playlist_expanded(self, event: PlaylistExpanded) -> None:
        record = await self._require_batch(event.batch_id)
        record.title = event.playlist_title
        record.thumbnail_url = event.playlist_thumbnail
        await self._batches.update(record)

    async def _on_playlist_expansion_failed(
        self, event: PlaylistExpansionFailed
    ) -> None:
        record = await self._require_batch(event.batch_id)
        if record.error is not None:
            if record.error == event.error and record.updated_at == event.occurred_at:
                return
            raise TrackingError("conflicting batch terminal event")
        pending = await self._jobs.list_by_batch(event.batch_id)
        if record.jobs_created or pending:
            raise TrackingError("invalid batch transition")
        record.error = event.error
        record.updated_at = event.occurred_at
        await self._batches.update(record)

    async def _require_job(self, job_id: UUID) -> JobRow:
        row = await self._jobs.get(job_id)
        if row is None:
            raise TrackingError("unknown job")
        return row

    async def _require_batch(self, batch_id: UUID) -> BatchRow:
        row = await self._batches.get(batch_id)
        if row is None:
            raise TrackingError("unknown batch")
        return row

    async def _touch_batch(self, batch: BatchRow, occurred_at: datetime) -> None:
        if occurred_at > batch.updated_at:
            batch.updated_at = occurred_at
            await self._batches.update(batch)

    async def _touch_batch_for_job(self, job: JobRow, occurred_at: datetime) -> None:
        if job.batch_id is None:
            return
        batch = await self._require_batch(job.batch_id)
        await self._touch_batch(batch, occurred_at)

    @staticmethod
    def _derive_batch_status(
        record: BatchRow, job_ids: tuple[UUID, ...], counts: dict[JobStatus, int]
    ) -> BatchStatus:
        if record.error is not None:
            return BatchStatus.FAILED
        if not job_ids:
            return BatchStatus.EXPANDING
        nonterminal = (
            counts[JobStatus.WAITING]
            + counts[JobStatus.PRODUCING]
            + counts[JobStatus.UPLOADING]
            + counts[JobStatus.INTERRUPTED]
        )
        if nonterminal:
            if counts[JobStatus.WAITING] == len(job_ids):
                return BatchStatus.WAITING
            return BatchStatus.PROCESSING
        if counts[JobStatus.FAILED] == len(job_ids):
            return BatchStatus.FAILED
        if counts[JobStatus.COMPLETED] == len(job_ids):
            if record.skipped_entries:
                return BatchStatus.PARTIALLY_COMPLETED
            return BatchStatus.COMPLETED
        return BatchStatus.PARTIALLY_COMPLETED


def _snapshot_from_row(row: JobRow) -> JobSnapshot:
    return JobSnapshot(
        id=row.id,
        batch_id=row.batch_id,
        source_kind=row.source_kind,
        source=row.source,
        title=row.title,
        status=row.status,
        artifact_id=row.artifact_id,
        filename=row.filename,
        size_bytes=row.size_bytes,
        telegram_chat_id=row.telegram_chat_id,
        telegram_message_id=row.telegram_message_id,
        error=row.error,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )
