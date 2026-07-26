"""Application wiring: build the components, connect them to one event bus."""

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from pyee.asyncio import AsyncIOEventEmitter

from .api.jobs import create_jobs_app
from .artifacts.cleanup import ArtifactCleanup
from .artifacts.storage import ArtifactStorage
from .config import Settings
from .downloaders.process import YouTubeProcessRunner
from .downloaders.youtube import YouTubeArtifactProducer
from .events import ERROR, TELEGRAM_UNAVAILABLE
from .jobs.scheduler import JobScheduler
from .jobs.tracker import JobTracker
from .telegram.client import TelegramClientAdapter
from .telegram.uploader import TelegramArtifactUploader


_LOGGER = logging.getLogger(__name__)
APPLICATION_BASE = Path(__file__).parent.parent


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


async def _shutdown(service: FastAPI) -> None:
    state = service.state
    state.readiness.close()
    state.scheduler.stop()
    state.uploader.begin_shutdown()
    await _drain_bus(state.bus, state.settings.shutdown_grace_seconds)
    await state.uploader.stop(disconnect=False)
    state.storage.clear_orphans()
    await state.telegram.disconnect()


def create_app(settings: Settings | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(service: FastAPI):
        resolved = settings if settings is not None else Settings.from_env(
            APPLICATION_BASE
        )
        state = service.state
        state.settings = resolved
        state.readiness = Readiness()
        state.storage = ArtifactStorage(resolved.artifact_root)
        state.storage.clear_orphans()
        state.bus = AsyncIOEventEmitter()
        state.tracker = JobTracker()
        state.tracker.register(state.bus)
        state.scheduler = JobScheduler(state.bus, state.storage)
        state.cleanup = ArtifactCleanup(state.storage)
        state.cleanup.register(state.bus)
        state.youtube = YouTubeArtifactProducer(
            state.bus, state.storage, YouTubeProcessRunner(), resolved
        )
        state.telegram = TelegramClientAdapter(resolved)
        state.uploader = TelegramArtifactUploader(
            state.bus, state.storage, state.telegram, resolved
        )

        def on_event_error(_error: object) -> None:
            _LOGGER.exception("Application event handler failed")
            state.readiness.close()
            state.scheduler.fail()

        state.bus.on(ERROR, on_event_error)
        state.bus.on(TELEGRAM_UNAVAILABLE, lambda _event: state.readiness.close())

        try:
            await state.uploader.start()
        except BaseException:
            await _shutdown(service)
            raise
        state.readiness.open()
        try:
            yield
        finally:
            await _shutdown(service)

    service = create_jobs_app()
    service.router.lifespan_context = lifespan
    return service


app = create_app()
