"""Application wiring: build the components, connect them to one event bus."""

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path

from fastapi import FastAPI
from pyee.asyncio import AsyncIOEventEmitter

from .api.jobs import create_jobs_app
from .artifacts.cleanup import ArtifactCleanup
from .artifacts.storage import ArtifactStorage
from .config import Settings
from .domain import JobStatus
from .downloaders.process import YouTubeProcessRunner
from .downloaders.youtube import YouTubeArtifactProducer
from .events import ERROR, TELEGRAM_UNAVAILABLE
from .jobs.progress import ProgressRegistry
from .jobs.repositories import (
    BatchesRepository,
    BatchQueueRepository,
    JobQueueRepository,
    JobQueueRow,
    JobsRepository,
    open_database,
)
from .jobs.scheduler import JobScheduler
from .jobs.tracker import JobTracker
from .telegram.client import TelegramClientAdapter
from .telegram.uploader import TelegramArtifactUploader
from .tui import install_log_handler


_LOGGER = logging.getLogger(__name__)
APPLICATION_BASE = Path(__file__).parent.parent
_LOG_FORMAT = "%(asctime)s %(levelname)-8s %(name)s %(message)s"


def _configure_logging(level: str) -> None:
    """Give the root logger a handler, then set our level only.

    Without a root handler, logging's last-resort fallback drops everything
    below WARNING. basicConfig is a no-op if handlers already exist, so an
    outer process that configured logging keeps its own setup. The level is
    set on our package alone so DEBUG does not also unleash Telethon.

    On a terminal, rich takes the handler so records render above a live
    progress bar instead of tearing through it.
    """
    if not install_log_handler(logging.WARNING):
        logging.basicConfig(format=_LOG_FORMAT, level=logging.WARNING)
    logging.getLogger("anything2telegram").setLevel(level)


class Readiness:
    """Whether the API may admit new work."""

    def __init__(self) -> None:
        self._accepting = False

    def open(self) -> None:
        self._accepting = True

    def close(self) -> None:
        self._accepting = False

    def is_accepting(self) -> bool:
        return self._accepting


async def _drain_bus(bus: AsyncIOEventEmitter, grace_seconds: float | int) -> None:
    """Let in-flight handlers finish, then cancel whatever is still running."""

    async def wait_until_complete() -> None:
        # A handler can emit again, so re-check until the bus stays idle.
        while not bus.complete:
            await bus.wait_for_complete()

    try:
        await asyncio.wait_for(wait_until_complete(), timeout=grace_seconds)
    except asyncio.TimeoutError:
        bus.cancel()


async def _recover_interrupted_jobs(
    jobs: JobsRepository, job_queue: JobQueueRepository
) -> None:
    """Retry, once, every job caught mid-flight when the process last died.

    No failure fact was ever recorded for these, so FAILED (an observed,
    never-auto-retried failure) would be the wrong status — INTERRUPTED is
    assigned here, at startup, specifically so this recovery can safely
    re-queue them from scratch under the same job_id.
    """
    stuck = await jobs.list_by_status((JobStatus.PRODUCING, JobStatus.UPLOADING))
    for row in stuck:
        row.status = JobStatus.INTERRUPTED
        row.updated_at = datetime.now(UTC)
        await jobs.update(row)
        if row.staged:
            queue_row = JobQueueRow(
                job_id=row.id,
                kind="staged",
                artifact_id=row.artifact_id,
                local_path=row.ready_local_path,
                filename=row.filename,
                media_type=row.ready_media_type,
                size_bytes=row.size_bytes,
                caption=row.ready_caption,
            )
        else:
            queue_row = JobQueueRow(
                job_id=row.id, kind="youtube", source_url=row.source, title=row.title
            )
        await job_queue.enqueue(queue_row)


async def _shutdown(service: FastAPI) -> None:
    state = service.state
    state.readiness.close()
    await state.scheduler.stop()
    state.uploader.begin_shutdown()
    await _drain_bus(state.bus, state.settings.shutdown_grace_seconds)
    await state.uploader.stop(disconnect=False)
    state.storage.clear_orphans()
    await state.telegram.disconnect()
    await state.db.close()


def create_app(settings: Settings | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(service: FastAPI):
        # Handlers first: parsing the config logs which browser profile the
        # YouTube cookies came from, and those records are lost to logging's
        # last-resort handler if no handler exists when they are emitted.
        _configure_logging("INFO")
        resolved = settings if settings is not None else Settings.from_env(
            APPLICATION_BASE
        )
        logging.getLogger("anything2telegram").setLevel(resolved.log_level)
        state = service.state
        state.settings = resolved
        state.readiness = Readiness()
        state.storage = ArtifactStorage(resolved.artifact_root)
        state.storage.clear_orphans()
        state.db = await open_database(resolved.db_path)
        jobs_repo = JobsRepository(state.db)
        batches_repo = BatchesRepository(state.db)
        job_queue_repo = JobQueueRepository(state.db)
        batch_queue_repo = BatchQueueRepository(state.db)
        state.bus = AsyncIOEventEmitter()
        state.tracker = JobTracker(jobs_repo, batches_repo)
        state.tracker.register(state.bus)
        state.progress = ProgressRegistry()
        state.scheduler = JobScheduler(
            state.bus, state.storage, job_queue_repo, batch_queue_repo
        )
        await _recover_interrupted_jobs(jobs_repo, job_queue_repo)
        state.cleanup = ArtifactCleanup(state.storage)
        state.cleanup.register(state.bus)
        state.youtube = YouTubeArtifactProducer(
            state.bus, state.storage, YouTubeProcessRunner(), resolved, state.progress
        )
        state.telegram = TelegramClientAdapter(resolved)
        state.uploader = TelegramArtifactUploader(
            state.bus, state.storage, state.telegram, resolved, state.progress
        )

        def on_event_error(error: object) -> None:
            # pyee re-emits handler exceptions from a done callback, so there is
            # no active exception context here — exc_info must be passed in or
            # the traceback is lost.
            _LOGGER.error(
                "Application event handler failed, closing admission",
                exc_info=error if isinstance(error, BaseException) else None,
            )
            state.readiness.close()
            state.scheduler.fail()

        state.bus.on(ERROR, on_event_error)
        state.bus.on(TELEGRAM_UNAVAILABLE, lambda _event: state.readiness.close())

        try:
            await state.uploader.start()
        except BaseException:
            await _shutdown(service)
            raise
        state.scheduler.start()
        state.readiness.open()
        try:
            yield
        finally:
            await _shutdown(service)

    service = create_jobs_app()
    service.router.lifespan_context = lifespan
    return service


app = create_app()
