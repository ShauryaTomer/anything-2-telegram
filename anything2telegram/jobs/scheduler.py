import asyncio
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID, uuid4

from ..artifacts.storage import ArtifactStorageError
from ..bus import EventBus
from ..domain import (
    ErrorInfo,
    JobPhase,
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
    ArtifactUploadFailed,
    ArtifactUploaded,
    BatchCreated,
    BatchJobsCreated,
    JobQueued,
    JobStarted,
    PlaylistExpanded,
    PlaylistExpansionFailed,
    PlaylistExpansionRequested,
    TelegramUnavailable,
    YouTubeDownloadRequested,
)


@dataclass(frozen=True)
class _YouTubeWork:
    job_id: UUID
    source_url: str
    causal_at: datetime


@dataclass(frozen=True)
class _PlaylistWork:
    batch_id: UUID
    source_url: str
    causal_at: datetime


@dataclass(frozen=True)
class _StagedWork:
    staged: StagedArtifact
    causal_at: datetime
    identity: tuple[int, int]

    @property
    def job_id(self) -> UUID:
        return self.staged.job_id


_Work = _YouTubeWork | _PlaylistWork | _StagedWork


class _ReservationStorage(Protocol):
    def reserve(
        self,
        job_id: UUID,
        artifact_id: UUID,
        filename: str,
        media_type: str | None,
        caption: str | None,
    ) -> UploadReservation: ...

    def delete_job_directory(self, job_id: UUID) -> None: ...

    def validate_staged_artifact(
        self,
        staged: StagedArtifact,
        expected_identity: tuple[int, int] | None = None,
    ) -> tuple[int, int]: ...


class SchedulerError(Exception):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(message)


class JobScheduler:
    def __init__(
        self,
        bus: EventBus,
        storage: _ReservationStorage | None = None,
        *,
        id_factory: Callable[[], UUID] = uuid4,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._bus = bus
        self._storage = storage
        self._id_factory = id_factory
        self._clock = clock
        self._queue: deque[_Work] = deque()
        self._active: _Work | None = None
        self._active_phase: JobPhase | None = None
        self._active_artifact_id: UUID | None = None
        self._active_causal_at: datetime | None = None
        self._deferred_facts: deque[tuple[str, object]] = deque()
        self._reservations: dict[UUID, UploadReservation] = {}
        self._pump_scheduled = False
        self._paused = False
        self._stopped = False
        bus.on(ARTIFACT_READY, self._on_artifact_ready)
        bus.on(ARTIFACT_PRODUCTION_FAILED, self._on_job_terminal)
        bus.on(ARTIFACT_UPLOADED, self._on_job_terminal)
        bus.on(ARTIFACT_UPLOAD_FAILED, self._on_job_terminal)
        bus.on(YOUTUBE_PLAYLIST_EXPANDED, self._on_playlist_expanded)
        bus.on(
            YOUTUBE_PLAYLIST_EXPANSION_FAILED,
            self._on_playlist_expansion_failed,
        )
        bus.on(TELEGRAM_UNAVAILABLE, self._on_telegram_unavailable)

    @property
    def active_id(self) -> UUID | None:
        if self._active is None:
            return None
        if isinstance(self._active, _YouTubeWork):
            return self._active.job_id
        if isinstance(self._active, _StagedWork):
            return self._active.job_id
        return self._active.batch_id

    @property
    def pending_count(self) -> int:
        return len(self._queue)

    @property
    def paused(self) -> bool:
        return self._paused

    @property
    def stopped(self) -> bool:
        return self._stopped

    def submit_youtube_video(self, source_url: str) -> UUID:
        asyncio.get_running_loop()
        self._require_accepting()
        self._require_source_url(source_url)
        job_id = self._id_factory()
        occurred_at = self._clock()
        event = JobQueued(
            job_id,
            None,
            SourceKind.YOUTUBE,
            source_url,
            None,
            occurred_at,
        )
        self._queue.append(_YouTubeWork(job_id, source_url, occurred_at))
        self._bus.emit(JOB_QUEUED, event)
        self._request_pump()
        return job_id

    def submit_playlist_expansion(self, source_url: str) -> UUID:
        asyncio.get_running_loop()
        self._require_accepting()
        self._require_source_url(source_url)
        batch_id = self._id_factory()
        occurred_at = self._clock()
        event = BatchCreated(batch_id, source_url, occurred_at)
        self._queue.append(_PlaylistWork(batch_id, source_url, occurred_at))
        self._bus.emit(BATCH_CREATED, event)
        self._request_pump()
        return batch_id

    def reserve_local_upload(
        self,
        filename: str,
        media_type: str | None,
        caption: str | None,
    ) -> UploadReservation:
        self._require_accepting()
        if self._storage is None:
            raise SchedulerError(
                "storage_required", "Local upload storage is unavailable"
            )
        job_id = self._id_factory()
        artifact_id = self._id_factory()
        reservation = self._storage.reserve(
            job_id, artifact_id, filename, media_type, caption
        )
        self._reservations[job_id] = reservation
        return reservation

    def enqueue_reserved_upload(self, staged: StagedArtifact) -> UUID:
        asyncio.get_running_loop()
        self._require_accepting()
        if not isinstance(staged, StagedArtifact):
            raise SchedulerError(
                "invalid_reservation", "Upload reservation is invalid"
            )
        reservation = self._reservations.get(staged.job_id)
        if reservation is None:
            raise SchedulerError(
                "invalid_reservation", "Upload reservation is unknown"
            )
        if (
            staged.artifact_id != reservation.artifact_id
            or staged.local_path != reservation.destination
            or staged.filename != reservation.filename
            or staged.media_type != reservation.media_type
            or staged.caption != reservation.caption
        ):
            raise SchedulerError(
                "invalid_reservation", "Upload reservation does not match"
            )
        if self._storage is None:
            raise SchedulerError(
                "upload_not_staged", "Local upload has not been staged"
            )
        try:
            identity = self._storage.validate_staged_artifact(staged)
        except ArtifactStorageError:
            raise SchedulerError(
                "upload_not_staged", "Local upload has not been staged"
            ) from None

        occurred_at = self._clock()
        event = JobQueued(
            staged.job_id,
            None,
            SourceKind.LOCAL_UPLOAD,
            staged.filename,
            staged,
            occurred_at,
        )
        del self._reservations[staged.job_id]
        self._queue.append(_StagedWork(staged, occurred_at, identity))
        self._bus.emit(JOB_QUEUED, event)
        self._request_pump()
        return staged.job_id

    def resume(self) -> None:
        if self._stopped or not self._paused:
            return
        self._paused = False
        self._request_pump()

    def stop(self) -> None:
        self._stopped = True
        for job_id in tuple(self._reservations):
            self.cancel_local_upload(job_id)
        for work in tuple(self._queue):
            if isinstance(work, _StagedWork):
                self.cancel_local_upload(work.job_id)

    def cancel_local_upload(self, job_id: UUID) -> bool:
        if not isinstance(job_id, UUID):
            raise TypeError("job_id must be UUID")
        if isinstance(self._active, _StagedWork) and self._active.job_id == job_id:
            return False
        if self._storage is None:
            return False
        reservation = self._reservations.get(job_id)
        if reservation is not None:
            self._storage.delete_job_directory(job_id)
            del self._reservations[job_id]
            return True
        waiting = next(
            (
                work
                for work in self._queue
                if isinstance(work, _StagedWork) and work.job_id == job_id
            ),
            None,
        )
        if waiting is None:
            return False
        self._storage.delete_job_directory(job_id)
        self._queue.remove(waiting)
        return True

    def _require_accepting(self) -> None:
        if self._stopped:
            raise SchedulerError(
                "scheduler_stopped", "Scheduler has been stopped"
            )

    @staticmethod
    def _require_source_url(source_url: object) -> None:
        if not isinstance(source_url, str):
            raise TypeError("source_url must be str")
        if not source_url.strip():
            raise ValueError("source_url must not be blank")

    def _request_pump(self) -> None:
        if self._stopped or self._pump_scheduled:
            return
        self._pump_scheduled = True
        asyncio.get_running_loop().call_soon(self._pump)

    def _on_job_terminal(
        self,
        event: ArtifactProductionFailed | ArtifactUploaded | ArtifactUploadFailed,
    ) -> None:
        if (
            self._active is None
            or isinstance(self._active, _PlaylistWork)
            or event.job_id != self._active.job_id
            or self._active_causal_at is None
            or event.occurred_at < self._active_causal_at
        ):
            return
        if isinstance(event, ArtifactProductionFailed):
            if self._active_phase is not JobPhase.PRODUCING:
                return
        elif (
            self._active_phase is not JobPhase.UPLOADING
            or event.artifact_id != self._active_artifact_id
        ):
            return
        self._clear_active()
        self._request_pump()

    def _on_artifact_ready(self, event: ArtifactReady) -> None:
        if (
            not isinstance(self._active, _YouTubeWork)
            or self._active_phase is not JobPhase.PRODUCING
            or event.job_id != self._active.job_id
            or self._active_causal_at is None
            or event.occurred_at < self._active_causal_at
        ):
            return
        self._active_phase = JobPhase.UPLOADING
        self._active_artifact_id = event.artifact_id
        self._active_causal_at = event.occurred_at

    def _clear_active(self) -> None:
        self._active = None
        self._active_phase = None
        self._active_artifact_id = None
        self._active_causal_at = None

    def _on_playlist_expanded(self, event: PlaylistExpanded) -> None:
        if (
            not isinstance(self._active, _PlaylistWork)
            or event.batch_id != self._active.batch_id
            or self._active_causal_at is None
            or event.occurred_at < self._active_causal_at
        ):
            return
        unique_targets = []
        seen_urls: set[str] = set()
        for target in event.targets:
            if target.source_url in seen_urls:
                continue
            seen_urls.add(target.source_url)
            unique_targets.append(target)

        occurred_at = max(
            self._clock(), event.occurred_at, self._active.causal_at
        )
        if not unique_targets:
            self._deferred_facts.append(
                (
                    YOUTUBE_PLAYLIST_EXPANSION_FAILED,
                    PlaylistExpansionFailed(
                        event.batch_id,
                        ErrorInfo(
                            "playlist_empty",
                            "Playlist contains no downloadable videos",
                        ),
                        occurred_at,
                    ),
                )
            )
            self._clear_active()
            self._request_pump()
            return
        children = [
            _YouTubeWork(self._id_factory(), target.source_url, occurred_at)
            for target in unique_targets
        ]
        self._queue.extendleft(reversed(children))
        for child in children:
            self._deferred_facts.append(
                (
                    JOB_QUEUED,
                    JobQueued(
                        child.job_id,
                        event.batch_id,
                        SourceKind.YOUTUBE,
                        child.source_url,
                        None,
                        occurred_at,
                    ),
                )
            )
        self._deferred_facts.append(
            (
                BATCH_JOBS_CREATED,
                BatchJobsCreated(
                    event.batch_id,
                    tuple(child.job_id for child in children),
                    event.skipped_entries + len(event.targets) - len(children),
                    occurred_at,
                ),
            )
        )
        self._clear_active()
        self._request_pump()

    def _on_playlist_expansion_failed(
        self, event: PlaylistExpansionFailed
    ) -> None:
        if (
            not isinstance(self._active, _PlaylistWork)
            or event.batch_id != self._active.batch_id
            or self._active_causal_at is None
            or event.occurred_at < self._active_causal_at
        ):
            return
        self._clear_active()
        self._request_pump()

    def _on_telegram_unavailable(self, event: TelegramUnavailable) -> None:
        self._paused = True

    def _pump(self) -> None:
        self._pump_scheduled = False
        if self._stopped:
            return
        while self._deferred_facts:
            topic, event = self._deferred_facts.popleft()
            self._bus.emit(topic, event)
        if (
            self._stopped
            or self._paused
            or self._active is not None
            or not self._queue
        ):
            return
        work = self._queue.popleft()
        self._active = work
        if isinstance(work, _PlaylistWork):
            self._active_phase = None
            self._active_artifact_id = None
            occurred_at = self._at_or_after(work.causal_at)
            self._active_causal_at = occurred_at
            self._bus.emit(
                YOUTUBE_PLAYLIST_EXPANSION_REQUESTED,
                PlaylistExpansionRequested(
                    work.batch_id, work.source_url, occurred_at
                ),
            )
            return
        if isinstance(work, _StagedWork):
            staged = work.staged
            self._active_phase = JobPhase.UPLOADING
            self._active_artifact_id = staged.artifact_id
            started_at = self._at_or_after(work.causal_at)
            self._active_causal_at = started_at
            self._bus.emit(
                JOB_STARTED,
                JobStarted(staged.job_id, JobPhase.UPLOADING, started_at),
            )
            ready_at = self._at_or_after(started_at)
            self._active_causal_at = ready_at
            try:
                if self._storage is None:
                    raise ArtifactStorageError(
                        "invalid_reservation",
                        "Upload reservation is invalid",
                    )
                self._storage.validate_staged_artifact(
                    staged, work.identity
                )
            except ArtifactStorageError:
                self._bus.emit(
                    ARTIFACT_UPLOAD_FAILED,
                    ArtifactUploadFailed(
                        staged.job_id,
                        staged.artifact_id,
                        ErrorInfo(
                            "artifact_invalid",
                            "Staged artifact is unavailable",
                        ),
                        ready_at,
                    ),
                )
                if self._storage is not None:
                    try:
                        self._storage.delete_job_directory(staged.job_id)
                    except ArtifactStorageError:
                        pass
                return
            self._bus.emit(
                ARTIFACT_READY,
                ArtifactReady(
                    staged.job_id,
                    staged.artifact_id,
                    staged.local_path,
                    staged.filename,
                    staged.media_type,
                    staged.size_bytes,
                    staged.caption,
                    ready_at,
                ),
            )
            return
        self._active_phase = JobPhase.PRODUCING
        self._active_artifact_id = None
        started_at = self._at_or_after(work.causal_at)
        self._active_causal_at = started_at
        self._bus.emit(
            JOB_STARTED,
            JobStarted(work.job_id, JobPhase.PRODUCING, started_at),
        )
        requested_at = self._at_or_after(started_at)
        self._active_causal_at = requested_at
        self._bus.emit(
            YOUTUBE_DOWNLOAD_REQUESTED,
            YouTubeDownloadRequested(
                work.job_id, work.source_url, requested_at
            ),
        )

    def _at_or_after(self, causal_at: datetime) -> datetime:
        return max(self._clock(), causal_at)
