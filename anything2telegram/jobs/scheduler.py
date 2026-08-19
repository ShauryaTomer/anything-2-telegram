"""Serial job queue: one artifact is produced and uploaded at a time."""

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID, uuid4

from ..artifacts.storage import ArtifactStorage, ArtifactStorageError
from ..domain import (
    BatchRef,
    ErrorInfo,
    JobSnapshot,
    JobPhase,
    SourceKind,
    JobRef,
    StagedArtifact,
    UploadReservation,
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
    TELEGRAM_UNAVAILABLE,
    YOUTUBE_DOWNLOAD_REQUESTED,
    YOUTUBE_PLAYLIST_EXPANDED,
    YOUTUBE_PLAYLIST_EXPANSION_FAILED,
    YOUTUBE_PLAYLIST_EXPANSION_REQUESTED,
    ArtifactProductionFailed,
    ArtifactReady,
    ArtifactUploaded,
    ArtifactUploadFailed,
    BatchCreated,
    BatchJobsCreated,
    JobQueued,
    JobStarted,
    PlaylistExpanded,
    PlaylistExpansionFailed,
    PlaylistExpansionRequested,
    YouTubeDownloadRequested,
)
from .repositories import BatchQueueRepository, JobQueueRepository, JobQueueRow, PlaylistQueueRow
from .tracker import JobTracker


_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class _YouTubeWork:
    job_id: UUID
    source_url: str
    caption_prefix: str = ""
    title: str | None = None


@dataclass(frozen=True)
class _PlaylistWork:
    batch_id: UUID
    source_url: str
    offset: int = 0


@dataclass(frozen=True)
class _StagedWork:
    staged: StagedArtifact

    @property
    def job_id(self) -> UUID:
        return self.staged.job_id


_Work = _YouTubeWork | _PlaylistWork | _StagedWork


class SchedulerError(Exception):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(message)


def _unavailable() -> SchedulerError:
    return SchedulerError("scheduler_unavailable", "Scheduler is unavailable")


def _work_to_job_queue_row(work: _YouTubeWork | _StagedWork) -> JobQueueRow:
    if isinstance(work, _StagedWork):
        staged = work.staged
        return JobQueueRow(
            job_id=staged.job_id,
            kind="staged",
            artifact_id=staged.artifact_id,
            local_path=staged.local_path,
            filename=staged.filename,
            media_type=staged.media_type,
            size_bytes=staged.size_bytes,
            caption=staged.caption,
        )
    return JobQueueRow(
        job_id=work.job_id,
        kind="youtube",
        source_url=work.source_url,
        caption_prefix=work.caption_prefix,
        title=work.title,
    )


def _row_to_work(row: JobQueueRow) -> _YouTubeWork | _StagedWork:
    if row.kind == "staged":
        staged = StagedArtifact(
            row.job_id,
            row.artifact_id,
            row.local_path,
            row.filename,
            row.media_type,
            row.size_bytes,
            row.caption,
        )
        return _StagedWork(staged)
    return _YouTubeWork(row.job_id, row.source_url, row.caption_prefix, row.title)


class JobScheduler:
    def __init__(
        self,
        bus,
        storage: ArtifactStorage,
        job_queue: JobQueueRepository,
        batch_queue: BatchQueueRepository,
        tracker: JobTracker | None = None,
    ) -> None:
        self._bus = bus
        self._storage = storage
        self._job_queue = job_queue
        self._batch_queue = batch_queue
        self._tracker = tracker
        self._active: _Work | None = None
        self._active_phase: JobPhase | None = None
        self._active_artifact_id: UUID | None = None
        self._reservations: dict[UUID, UploadReservation] = {}
        self._pump_scheduled = False
        self._pumping = False
        self._paused = False
        self._stopped = False
        bus.on(ARTIFACT_READY, self._on_artifact_ready)
        bus.on(ARTIFACT_PRODUCTION_FAILED, self._on_job_terminal)
        bus.on(ARTIFACT_UPLOADED, self._on_job_terminal)
        bus.on(ARTIFACT_UPLOAD_FAILED, self._on_job_terminal)
        bus.on(YOUTUBE_PLAYLIST_EXPANDED, self._on_playlist_expanded)
        bus.on(YOUTUBE_PLAYLIST_EXPANSION_FAILED, self._on_playlist_settled)
        bus.on(TELEGRAM_UNAVAILABLE, self._on_telegram_unavailable)

    @property
    def accepting(self) -> bool:
        return not (self._paused or self._stopped)

    def start(self) -> None:
        """Kick the pump once at boot, in case startup recovery seeded the queue.

        Every other path into the queue (submit_video, submit_playlist,
        enqueue_reserved_upload, and the terminal-fact handlers) already
        requests a pump itself; recovery is the one way to populate the
        queue without going through any of them.
        """
        self._request_pump()

    async def submit_video(self, source_url: str) -> JobRef:
        self._require_accepting()
        job_id = uuid4()
        await self._job_queue.enqueue(JobQueueRow(job_id=job_id, kind="youtube", source_url=source_url))
        self._emit(
            JOB_QUEUED,
            JobQueued(job_id, None, SourceKind.YOUTUBE, source_url, None, _now()),
        )
        self._request_pump()
        return JobRef(job_id, f"/jobs/{job_id}")

    async def submit_playlist(self, source_url: str, offset: int = 0) -> BatchRef:
        self._require_accepting()
        batch_id = uuid4()
        await self._batch_queue.enqueue(PlaylistQueueRow(batch_id, source_url, offset))
        self._emit(BATCH_CREATED, BatchCreated(batch_id, source_url, _now()))
        self._request_pump()
        return BatchRef(batch_id, f"/batches/{batch_id}")

    async def reserve_local_upload(
        self,
        filename: str,
        media_type: str | None,
        caption: str | None,
    ) -> UploadReservation:
        self._require_accepting()
        job_id = uuid4()
        reservation = self._storage.reserve(
            job_id, uuid4(), filename, media_type, caption
        )
        self._reservations[job_id] = reservation
        return reservation

    async def enqueue_reserved_upload(
        self, reservation: UploadReservation, size_bytes: int
    ) -> JobRef:
        self._require_accepting()
        if self._reservations.get(reservation.job_id) != reservation:
            raise SchedulerError(
                "invalid_reservation", "Upload reservation is unknown"
            )
        staged = StagedArtifact(
            reservation.job_id,
            reservation.artifact_id,
            reservation.destination,
            reservation.filename,
            reservation.media_type,
            size_bytes,
            reservation.caption,
        )
        try:
            self._storage.validate_staged_artifact(staged)
        except ArtifactStorageError:
            raise SchedulerError(
                "upload_not_staged", "Local upload has not been staged"
            ) from None

        del self._reservations[staged.job_id]
        await self._job_queue.enqueue(_work_to_job_queue_row(_StagedWork(staged)))
        self._emit(
            JOB_QUEUED,
            JobQueued(
                staged.job_id,
                None,
                SourceKind.LOCAL_UPLOAD,
                staged.filename,
                staged,
                _now(),
            ),
        )
        self._request_pump()
        return JobRef(staged.job_id, f"/jobs/{staged.job_id}")

    async def enqueue_batch_retries(self, jobs: tuple[JobSnapshot, ...]) -> None:
        self._require_accepting()
        rows = [
            JobQueueRow(
                job_id=job.id,
                kind="youtube",
                source_url=job.source,
                title=job.title,
            )
            for job in jobs
            if job.source_kind is SourceKind.YOUTUBE
        ]
        if not rows:
            return
        for row in rows:
            await self._job_queue.remove(row.job_id)
        await self._job_queue.prepend(rows)
        self._request_pump()

    async def retry_failed_batch(self, batch_id: UUID) -> tuple[JobSnapshot, ...]:
        if self._tracker is None:
            raise SchedulerError("retry_unavailable", "Retry is not configured")

        self._require_accepting()
        jobs = await self._tracker.claim_failed_batch_jobs_for_retry(batch_id)
        if not jobs:
            return ()

        retryable_jobs = tuple(job for job in jobs if job.source_kind is SourceKind.YOUTUBE)
        if not retryable_jobs:
            await self._tracker.revert_batch_jobs_to_failed(jobs)
            return ()

        try:
            await self.enqueue_batch_retries(retryable_jobs)
        except Exception:
            await self._tracker.revert_batch_jobs_to_failed(jobs)
            raise

        non_retryable_jobs = tuple(
            job for job in jobs if job.source_kind is not SourceKind.YOUTUBE
        )
        if non_retryable_jobs:
            await self._tracker.revert_batch_jobs_to_failed(non_retryable_jobs)

        await self._tracker.clear_retried_batch_job_state(retryable_jobs)
        return retryable_jobs

    async def cancel_reserved_upload(self, job_id: UUID) -> bool:
        if job_id not in self._reservations:
            return False
        self._release_reservation(job_id)
        return True

    def pause(self) -> None:
        if not self._paused:
            _LOGGER.warning("Scheduler paused")
        self._paused = True

    def _mark_stopped(self) -> None:
        self._stopped = True
        for job_id in tuple(self._reservations):
            self._release_reservation(job_id)

    async def _drop_queued_staged_uploads(self) -> None:
        for entry in await self._job_queue.list_all():
            if entry.kind == "staged":
                self._storage.delete_job_directory(entry.job_id)
                await self._job_queue.remove(entry.job_id)

    async def stop(self) -> None:
        self._mark_stopped()
        await self._drop_queued_staged_uploads()

    def fail(self) -> None:
        """A handler crashed. Same as stop(), plus drop the active upload's files.

        Stays sync (unlike stop()) because it is called from the sync `error`
        bus listener in main.py and from `_emit`'s except block — both need
        `accepting` to flip immediately. The queued-upload DB cleanup that
        `stop()` awaits happens here as a best-effort background task instead.
        """
        if self._stopped:
            return
        _LOGGER.error(
            "Scheduler failed, discarding queued job(s); active=%s",
            getattr(self._active, "job_id", None),
        )
        self._pump_scheduled = False
        self._mark_stopped()
        asyncio.get_running_loop().create_task(self._drop_queued_staged_uploads())
        if isinstance(self._active, _StagedWork):
            self._storage.delete_job_directory(self._active.job_id)

    def _release_reservation(self, job_id: UUID) -> None:
        self._storage.delete_job_directory(job_id)
        del self._reservations[job_id]

    def _require_accepting(self) -> None:
        if not self.accepting:
            raise _unavailable()

    def _emit(self, topic: str, event: object) -> None:
        """Publish a fact, then surface a scheduler failure a handler caused."""
        try:
            self._bus.emit(topic, event)
        except Exception:
            self.fail()
            raise
        if self._stopped:
            raise _unavailable()

    def _on_job_terminal(
        self,
        event: ArtifactProductionFailed | ArtifactUploaded | ArtifactUploadFailed,
    ) -> None:
        if (
            isinstance(self._active, (_YouTubeWork, _StagedWork))
            and event.job_id == self._active.job_id
            and self._matches_active_phase(event)
        ):
            self._clear_active()
            self._request_pump()

    def _matches_active_phase(self, event: object) -> bool:
        if isinstance(event, ArtifactProductionFailed):
            return self._active_phase is JobPhase.PRODUCING
        return (
            self._active_phase is JobPhase.UPLOADING
            and event.artifact_id == self._active_artifact_id
        )

    def _on_artifact_ready(self, event: ArtifactReady) -> None:
        if (
            isinstance(self._active, _YouTubeWork)
            and self._active_phase is JobPhase.PRODUCING
            and event.job_id == self._active.job_id
        ):
            self._active_phase = JobPhase.UPLOADING
            self._active_artifact_id = event.artifact_id

    def _clear_active(self) -> None:
        self._active = None
        self._active_phase = None
        self._active_artifact_id = None

    async def _on_playlist_expanded(self, event: PlaylistExpanded) -> None:
        if (
            not isinstance(self._active, _PlaylistWork)
            or event.batch_id != self._active.batch_id
        ):
            return
        children = [
            _YouTubeWork(uuid4(), target.source_url, target.caption_prefix, target.title)
            for target in event.targets
        ]
        await self._job_queue.prepend([_work_to_job_queue_row(child) for child in children])
        self._clear_active()
        for child in children:
            self._emit(
                JOB_QUEUED,
                JobQueued(
                    child.job_id,
                    event.batch_id,
                    SourceKind.YOUTUBE,
                    child.source_url,
                    None,
                    _now(),
                    title=child.title,
                ),
            )
        self._emit(
            BATCH_JOBS_CREATED,
            BatchJobsCreated(
                event.batch_id,
                tuple(child.job_id for child in children),
                event.skipped_entries,
                _now(),
            ),
        )
        self._request_pump()

    def _on_playlist_settled(self, event: PlaylistExpansionFailed) -> None:
        if (
            isinstance(self._active, _PlaylistWork)
            and event.batch_id == self._active.batch_id
        ):
            self._clear_active()
            self._request_pump()

    def _on_telegram_unavailable(self, event: object) -> None:
        self.pause()

    def _request_pump(self) -> None:
        if self._paused or self._stopped or self._pump_scheduled:
            return
        self._pump_scheduled = True
        asyncio.get_running_loop().create_task(self._pump())

    async def _next_work(self) -> _Work | None:
        job_sequence = await self._job_queue.peek_sequence()
        batch_sequence = await self._batch_queue.peek_sequence()
        if job_sequence is None and batch_sequence is None:
            return None
        if batch_sequence is not None and (job_sequence is None or batch_sequence < job_sequence):
            entry = await self._batch_queue.dequeue()
            return _PlaylistWork(entry.batch_id, entry.source_url, entry.offset)
        entry = await self._job_queue.dequeue()
        return _row_to_work(entry)

    async def _pump(self) -> None:
        self._pump_scheduled = False
        # `_next_work()` awaits real DB I/O, so a second pump task can start
        # and pass the `_active is None` check before this one has set
        # `_active` — this flag closes that window, held for the whole body.
        if self._pumping or not self.accepting or self._active is not None:
            return
        self._pumping = True
        try:
            work = await self._next_work()
            if work is None:
                return
            self._active = work
            self._active_artifact_id = None
            try:
                if isinstance(work, _PlaylistWork):
                    self._start_playlist(work)
                elif isinstance(work, _StagedWork):
                    self._start_upload(work)
                else:
                    self._start_download(work)
            except Exception:
                self.fail()
                raise
        finally:
            self._pumping = False

    def _start_playlist(self, work: _PlaylistWork) -> None:
        self._active_phase = None
        self._emit(
            YOUTUBE_PLAYLIST_EXPANSION_REQUESTED,
            PlaylistExpansionRequested(
                work.batch_id, work.source_url, _now(), work.offset
            ),
        )

    def _start_download(self, work: _YouTubeWork) -> None:
        self._active_phase = JobPhase.PRODUCING
        self._emit(JOB_STARTED, JobStarted(work.job_id, JobPhase.PRODUCING, _now()))
        self._emit(
            YOUTUBE_DOWNLOAD_REQUESTED,
            YouTubeDownloadRequested(
                work.job_id, work.source_url, _now(), work.caption_prefix
            ),
        )

    def _start_upload(self, work: _StagedWork) -> None:
        staged = work.staged
        self._active_phase = JobPhase.UPLOADING
        self._active_artifact_id = staged.artifact_id
        self._emit(
            JOB_STARTED, JobStarted(staged.job_id, JobPhase.UPLOADING, _now())
        )
        try:
            self._storage.validate_staged_artifact(staged)
        except ArtifactStorageError:
            self._storage.delete_job_directory(staged.job_id)
            self._emit(
                ARTIFACT_UPLOAD_FAILED,
                ArtifactUploadFailed(
                    staged.job_id,
                    staged.artifact_id,
                    ErrorInfo("artifact_invalid", "Staged artifact is unavailable"),
                    _now(),
                ),
            )
            return
        self._emit(
            ARTIFACT_READY,
            ArtifactReady(
                staged.job_id,
                staged.artifact_id,
                staged.local_path,
                staged.filename,
                staged.media_type,
                staged.size_bytes,
                staged.caption,
                _now(),
            ),
        )


def _now() -> datetime:
    return datetime.now(UTC)
