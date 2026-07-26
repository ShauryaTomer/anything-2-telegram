"""Serial job queue: one artifact is produced and uploaded at a time."""

import asyncio
import logging
from collections import deque
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID, uuid4

from ..artifacts.storage import ArtifactStorage, ArtifactStorageError
from ..domain import (
    BatchRef,
    ErrorInfo,
    JobPhase,
    JobRef,
    SourceKind,
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


_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class _YouTubeWork:
    job_id: UUID
    source_url: str
    caption_prefix: str = ""


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


class JobScheduler:
    def __init__(self, bus, storage: ArtifactStorage) -> None:
        self._bus = bus
        self._storage = storage
        self._queue: deque[_Work] = deque()
        self._active: _Work | None = None
        self._active_phase: JobPhase | None = None
        self._active_artifact_id: UUID | None = None
        self._reservations: dict[UUID, UploadReservation] = {}
        self._pump_scheduled = False
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

    def submit_video(self, source_url: str) -> JobRef:
        self._require_accepting()
        job_id = uuid4()
        self._queue.append(_YouTubeWork(job_id, source_url))
        self._emit(
            JOB_QUEUED,
            JobQueued(job_id, None, SourceKind.YOUTUBE, source_url, None, _now()),
        )
        self._request_pump()
        return JobRef(job_id, f"/jobs/{job_id}")

    def submit_playlist(self, source_url: str, offset: int = 0) -> BatchRef:
        self._require_accepting()
        batch_id = uuid4()
        self._queue.append(_PlaylistWork(batch_id, source_url, offset))
        self._emit(BATCH_CREATED, BatchCreated(batch_id, source_url, _now()))
        self._request_pump()
        return BatchRef(batch_id, f"/batches/{batch_id}")

    def reserve_local_upload(
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

    def enqueue_reserved_upload(
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
        self._queue.append(_StagedWork(staged))
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

    def cancel_reserved_upload(self, job_id: UUID) -> bool:
        if job_id not in self._reservations:
            return False
        self._release_reservation(job_id)
        return True

    def pause(self) -> None:
        if not self._paused:
            _LOGGER.warning(
                "Scheduler paused, %d job(s) left waiting", len(self._queue)
            )
        self._paused = True

    def stop(self) -> None:
        self._stopped = True
        for job_id in tuple(self._reservations):
            self._release_reservation(job_id)
        for work in tuple(self._queue):
            if isinstance(work, _StagedWork):
                self._storage.delete_job_directory(work.job_id)
                self._queue.remove(work)

    def fail(self) -> None:
        if self._stopped:
            return
        _LOGGER.error(
            "Scheduler failed, discarding %d queued job(s); active=%s",
            len(self._queue),
            getattr(self._active, "job_id", None),
        )
        self._pump_scheduled = False
        self.stop()
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

    def _on_playlist_expanded(self, event: PlaylistExpanded) -> None:
        if (
            not isinstance(self._active, _PlaylistWork)
            or event.batch_id != self._active.batch_id
        ):
            return
        children = [
            _YouTubeWork(uuid4(), target.source_url, target.caption_prefix)
            for target in event.targets
        ]
        self._queue.extendleft(reversed(children))
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
        asyncio.get_running_loop().call_soon(self._pump)

    def _pump(self) -> None:
        self._pump_scheduled = False
        if not self.accepting or self._active is not None or not self._queue:
            return
        work = self._queue.popleft()
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
