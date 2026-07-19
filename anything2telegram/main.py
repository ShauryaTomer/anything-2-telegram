import asyncio
import logging
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from uuid import UUID

from fastapi import FastAPI
from pyee.asyncio import AsyncIOEventEmitter

from .artifacts.storage import ArtifactStorage, ArtifactStorageError
from .api.jobs import create_jobs_app
from .config import Settings
from .domain import BatchSnapshot, JobSnapshot, StagedArtifact, UploadReservation
from .downloaders.process import YouTubeProcessRunner
from .downloaders.youtube import YouTubeArtifactProducer
from .events import ERROR, TELEGRAM_UNAVAILABLE
from .jobs.scheduler import JobScheduler, SchedulerError
from .jobs.tracker import JobTracker
from .telegram.client import TelegramClientAdapter
from .telegram.uploader import TelegramArtifactUploader


_LOGGER = logging.getLogger(__name__)
_STATE_NAMES = (
    "settings",
    "storage",
    "bus",
    "readiness",
    "tracker",
    "scheduler",
    "process_runner",
    "youtube_producer",
    "telegram",
    "uploader",
)


class StorageAdapter(Protocol):
    root: Path

    def clear_orphans(self) -> None: ...

    async def stage(
        self, upload: object, reservation: UploadReservation, max_bytes: int
    ) -> StagedArtifact: ...


class ReadinessAdapter(Protocol):
    def open(self) -> None: ...

    def close(self) -> None: ...

    def is_accepting(self) -> bool: ...


class TrackerAdapter(Protocol):
    def register(self, bus: AsyncIOEventEmitter) -> None: ...

    def get_job(self, job_id: UUID) -> JobSnapshot | None: ...

    def get_batch(self, batch_id: UUID) -> BatchSnapshot | None: ...


class SchedulerAdapter(Protocol):
    accepting: bool

    def stop(self) -> None: ...

    def fail(self, error: object | None = None) -> None: ...


class TelegramAdapter(Protocol):
    def is_connected(self) -> bool: ...

    async def disconnect(self) -> None: ...


class UploaderAdapter(Protocol):
    def begin_shutdown(self) -> None: ...

    async def start(self) -> None: ...

    async def stop(self, *, disconnect: bool = True) -> None: ...


BusFactory = Callable[[], AsyncIOEventEmitter]
StorageFactory = Callable[[Settings], StorageAdapter]
ReadinessFactory = Callable[[], ReadinessAdapter]
TrackerFactory = Callable[[], TrackerAdapter]
SchedulerFactory = Callable[[AsyncIOEventEmitter, StorageAdapter], SchedulerAdapter]
ProcessRunnerFactory = Callable[[], object]
YouTubeFactory = Callable[
    [AsyncIOEventEmitter, StorageAdapter, object, Settings], object
]
TelegramFactory = Callable[[Settings], TelegramAdapter]
UploaderFactory = Callable[
    [AsyncIOEventEmitter, StorageAdapter, TelegramAdapter, Settings], UploaderAdapter
]


class Readiness:
    def __init__(self) -> None:
        self._accepting = False

    def open(self) -> None:
        self._accepting = True

    def close(self) -> None:
        self._accepting = False

    def is_accepting(self) -> bool:
        return self._accepting


def _storage(settings: Settings) -> ArtifactStorage:
    return ArtifactStorage(settings.artifact_root)


def _scheduler(
    bus: AsyncIOEventEmitter, storage: StorageAdapter
) -> JobScheduler:
    return JobScheduler(bus, storage)


def _youtube(
    bus: AsyncIOEventEmitter,
    storage: StorageAdapter,
    runner: object,
    settings: Settings,
) -> YouTubeArtifactProducer:
    return YouTubeArtifactProducer(
        bus,
        storage,
        runner,
        timeout_seconds=settings.ytdlp_timeout_seconds,
        cookies_path=settings.cookies_path,
    )


def _telegram(settings: Settings) -> TelegramClientAdapter:
    return TelegramClientAdapter(settings)


def _uploader(
    bus: AsyncIOEventEmitter,
    storage: StorageAdapter,
    telegram: TelegramAdapter,
    settings: Settings,
) -> TelegramArtifactUploader:
    return TelegramArtifactUploader(bus, storage, telegram, settings)


@dataclass(frozen=True)
class AdapterFactories:
    bus_factory: BusFactory = AsyncIOEventEmitter
    storage_factory: StorageFactory = _storage
    readiness_factory: ReadinessFactory = Readiness
    tracker_factory: TrackerFactory = JobTracker
    scheduler_factory: SchedulerFactory = _scheduler
    process_runner_factory: ProcessRunnerFactory = YouTubeProcessRunner
    youtube_factory: YouTubeFactory = _youtube
    telegram_factory: TelegramFactory = _telegram
    uploader_factory: UploaderFactory = _uploader


class _StateAccess:
    def __init__(self, holder: dict[str, FastAPI], name: str) -> None:
        self._holder = holder
        self._name = name

    def get(self) -> object | None:
        service = self._holder.get("app")
        if service is None:
            return None
        try:
            return getattr(service.state, self._name)
        except AttributeError:
            return None


class _ReadinessProxy:
    def __init__(self, access: _StateAccess) -> None:
        self._access = access

    def is_accepting(self) -> bool:
        target = self._access.get()
        return target is not None and target.is_accepting() is True


class _TelegramProxy:
    def __init__(self, access: _StateAccess) -> None:
        self._access = access

    def is_connected(self) -> bool:
        target = self._access.get()
        if target is None:
            return False
        state = getattr(target, "is_connected", False)
        return bool(state() if callable(state) else state)


class _TrackerProxy:
    def __init__(self, access: _StateAccess) -> None:
        self._access = access

    def get_job(self, job_id: UUID) -> JobSnapshot | None:
        target = self._access.get()
        return None if target is None else target.get_job(job_id)

    def get_batch(self, batch_id: UUID) -> BatchSnapshot | None:
        target = self._access.get()
        return None if target is None else target.get_batch(batch_id)


class _SchedulerProxy:
    def __init__(self, access: _StateAccess) -> None:
        self._access = access

    @property
    def accepting(self) -> bool:
        target = self._access.get()
        return target is not None and target.accepting is True

    def __getattr__(self, name: str):
        target = self._access.get()
        if target is None:
            raise SchedulerError(
                "scheduler_unavailable", "Scheduler is unavailable"
            )
        return getattr(target, name)


class _StorageProxy:
    def __init__(self, access: _StateAccess) -> None:
        self._access = access

    async def stage(
        self, upload: object, reservation: UploadReservation, max_bytes: int
    ) -> StagedArtifact:
        target = self._access.get()
        if target is None:
            raise ArtifactStorageError(
                "storage_unavailable", "Artifact storage is unavailable"
            )
        return await target.stage(upload, reservation, max_bytes)


def _safe_log(message: str) -> None:
    try:
        _LOGGER.error(message)
    except BaseException:
        pass


def _clear_state(service: FastAPI) -> None:
    for name in _STATE_NAMES:
        try:
            delattr(service.state, name)
        except AttributeError:
            pass


async def _drain_bus(bus: AsyncIOEventEmitter, grace_seconds: float | int) -> None:
    async def wait_until_complete() -> None:
        while not bus.complete:
            await bus.wait_for_complete()
            await asyncio.sleep(0)

    try:
        await asyncio.wait_for(wait_until_complete(), timeout=grace_seconds)
        return
    except asyncio.TimeoutError:
        pending = tuple(getattr(bus, "_waiting", ()))
        bus.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)


async def _shutdown_components(service: FastAPI) -> None:
    readiness = getattr(service.state, "readiness", None)
    scheduler = getattr(service.state, "scheduler", None)
    bus = getattr(service.state, "bus", None)
    settings = getattr(service.state, "settings", None)
    uploader = getattr(service.state, "uploader", None)
    storage = getattr(service.state, "storage", None)
    telegram = getattr(service.state, "telegram", None)

    if readiness is not None:
        readiness.close()
    if scheduler is not None:
        try:
            scheduler.stop()
        except Exception:
            _safe_log("Scheduler shutdown failed")
    if uploader is not None:
        try:
            begin_shutdown = getattr(uploader, "begin_shutdown", None)
            if callable(begin_shutdown):
                begin_shutdown()
        except Exception:
            _safe_log("Telegram uploader admission shutdown failed")
    if bus is not None and settings is not None:
        try:
            await _drain_bus(bus, settings.shutdown_grace_seconds)
        except Exception:
            _safe_log("Event drain failed")
    if uploader is not None:
        try:
            await uploader.stop(disconnect=False)
        except Exception:
            _safe_log("Telegram uploader shutdown failed")
    if storage is not None:
        try:
            storage.clear_orphans()
        except Exception:
            _safe_log("Artifact cleanup failed")
    try:
        if telegram is not None:
            await telegram.disconnect()
    except Exception:
        _safe_log("Telegram disconnect failed")
    finally:
        _clear_state(service)


async def _shielded_shutdown(service: FastAPI) -> None:
    task = asyncio.create_task(_shutdown_components(service))
    try:
        await asyncio.shield(task)
    except asyncio.CancelledError:
        await task
        raise


def create_app(
    settings: Settings | None = None,
    adapters: AdapterFactories | None = None,
) -> FastAPI:
    factories = adapters if adapters is not None else AdapterFactories()
    holder: dict[str, FastAPI] = {}
    scheduler_proxy = _SchedulerProxy(_StateAccess(holder, "scheduler"))
    tracker_proxy = _TrackerProxy(_StateAccess(holder, "tracker"))
    storage_proxy = _StorageProxy(_StateAccess(holder, "storage"))
    readiness_proxy = _ReadinessProxy(_StateAccess(holder, "readiness"))
    telegram_proxy = _TelegramProxy(_StateAccess(holder, "telegram"))

    @asynccontextmanager
    async def lifespan(service: FastAPI):
        resolved = settings
        if resolved is None:
            resolved = Settings.from_env(Path.cwd())
        service.state.settings = resolved

        try:
            storage = factories.storage_factory(resolved)
            service.state.storage = storage
            storage.clear_orphans()
            bus = factories.bus_factory()
            service.state.bus = bus
            readiness = factories.readiness_factory()
            service.state.readiness = readiness
            scheduler_ref: dict[str, SchedulerAdapter] = {}

            def unavailable(_event: object) -> None:
                readiness.close()

            def event_error(error: object) -> None:
                _safe_log("Application event handler failed")
                readiness.close()
                scheduler = scheduler_ref.get("scheduler")
                if scheduler is not None:
                    try:
                        scheduler.fail(error)
                    except BaseException:
                        pass

            bus.on(ERROR, event_error)
            bus.on(TELEGRAM_UNAVAILABLE, unavailable)
            tracker = factories.tracker_factory()
            service.state.tracker = tracker
            tracker.register(bus)
            scheduler = factories.scheduler_factory(bus, storage)
            scheduler_ref["scheduler"] = scheduler
            service.state.scheduler = scheduler
            process_runner = factories.process_runner_factory()
            service.state.process_runner = process_runner
            youtube_producer = factories.youtube_factory(
                bus, storage, process_runner, resolved
            )
            service.state.youtube_producer = youtube_producer
            telegram = factories.telegram_factory(resolved)
            service.state.telegram = telegram
            uploader = factories.uploader_factory(
                bus, storage, telegram, resolved
            )
            service.state.uploader = uploader
            await uploader.start()
            readiness.open()
        except BaseException:
            await _shielded_shutdown(service)
            raise

        try:
            yield
        finally:
            await _shielded_shutdown(service)

    service = create_jobs_app(
        scheduler=scheduler_proxy,
        tracker=tracker_proxy,
        storage=storage_proxy,
        readiness=readiness_proxy,
        telegram=telegram_proxy,
        max_upload_bytes=lambda: (
            getattr(holder["app"].state, "settings").max_artifact_bytes
            if hasattr(holder["app"].state, "settings")
            else 0
        ),
    )
    service.router.lifespan_context = lifespan
    holder["app"] = service
    return service


app = create_app()


__all__ = ["AdapterFactories", "Readiness", "app", "create_app"]
