import asyncio
import os
import stat
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID, uuid4

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


@dataclass(frozen=True)
class _PlaylistWork:
    batch_id: UUID
    source_url: str


@dataclass(frozen=True)
class _StagedWork:
    staged: StagedArtifact

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
        self._deferred_facts: deque[tuple[str, object]] = deque()
        self._reservations: dict[UUID, UploadReservation] = {}
        self._pump_scheduled = False
        self._paused = False
        self._stopped = False
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
        event = JobQueued(
            job_id,
            None,
            SourceKind.YOUTUBE,
            source_url,
            None,
            self._clock(),
        )
        self._queue.append(_YouTubeWork(job_id, source_url))
        self._bus.emit(JOB_QUEUED, event)
        self._request_pump()
        return job_id

    def submit_playlist_expansion(self, source_url: str) -> UUID:
        asyncio.get_running_loop()
        self._require_accepting()
        self._require_source_url(source_url)
        batch_id = self._id_factory()
        event = BatchCreated(batch_id, source_url, self._clock())
        self._queue.append(_PlaylistWork(batch_id, source_url))
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
        try:
            staged_stat = staged.local_path.lstat()
        except OSError:
            raise SchedulerError(
                "upload_not_staged", "Local upload has not been staged"
            ) from None
        if (
            not stat.S_ISREG(staged_stat.st_mode)
            or staged_stat.st_uid != os.getuid()
            or staged_stat.st_nlink != 1
            or staged_stat.st_size != staged.size_bytes
        ):
            raise SchedulerError(
                "upload_not_staged", "Local upload has not been staged"
            )

        event = JobQueued(
            staged.job_id,
            None,
            SourceKind.LOCAL_UPLOAD,
            staged.filename,
            staged,
            self._clock(),
        )
        del self._reservations[staged.job_id]
        self._queue.append(_StagedWork(staged))
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
        ):
            return
        self._active = None
        self._request_pump()

    def _on_playlist_expanded(self, event: PlaylistExpanded) -> None:
        if (
            not isinstance(self._active, _PlaylistWork)
            or event.batch_id != self._active.batch_id
        ):
            return
        unique_targets = []
        seen_urls: set[str] = set()
        for target in event.targets:
            if target.source_url in seen_urls:
                continue
            seen_urls.add(target.source_url)
            unique_targets.append(target)

        occurred_at = self._clock()
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
            self._active = None
            self._request_pump()
            return
        children = [
            _YouTubeWork(self._id_factory(), target.source_url)
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
        self._active = None
        self._request_pump()

    def _on_playlist_expansion_failed(
        self, event: PlaylistExpansionFailed
    ) -> None:
        if (
            not isinstance(self._active, _PlaylistWork)
            or event.batch_id != self._active.batch_id
        ):
            return
        self._active = None
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
            self._bus.emit(
                YOUTUBE_PLAYLIST_EXPANSION_REQUESTED,
                PlaylistExpansionRequested(
                    work.batch_id, work.source_url, self._clock()
                ),
            )
            return
        if isinstance(work, _StagedWork):
            staged = work.staged
            self._bus.emit(
                JOB_STARTED,
                JobStarted(staged.job_id, JobPhase.UPLOADING, self._clock()),
            )
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
                    self._clock(),
                ),
            )
            return
        self._bus.emit(
            JOB_STARTED,
            JobStarted(work.job_id, JobPhase.PRODUCING, self._clock()),
        )
        self._bus.emit(
            YOUTUBE_DOWNLOAD_REQUESTED,
            YouTubeDownloadRequested(
                work.job_id, work.source_url, self._clock()
            ),
        )
