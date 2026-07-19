import asyncio
from pathlib import Path
from uuid import UUID

import pytest
from pyee.asyncio import AsyncIOEventEmitter

import anything2telegram.main as main_module
from anything2telegram.artifacts.storage import ArtifactStorage
from anything2telegram.config import Settings
from anything2telegram.events import (
    ARTIFACT_READY,
    ERROR,
    JOB_QUEUED,
    TELEGRAM_UNAVAILABLE,
    YOUTUBE_DOWNLOAD_REQUESTED,
)
from anything2telegram.main import AdapterFactories, create_app


def make_settings(tmp_path: Path, *, grace: float = 0.01) -> Settings:
    return Settings(
        api_id=1,
        api_hash="redacted",
        bot_token="redacted",
        channel_id=-1001,
        session_path=tmp_path / "service.session",
        cookies_path=None,
        artifact_root=tmp_path / "artifacts",
        max_artifact_bytes=1024,
        ytdlp_timeout_seconds=1,
        tg_upload_timeout_seconds=1,
        shutdown_grace_seconds=grace,
    )


class RecordingBus(AsyncIOEventEmitter):
    def __init__(self, actions: list[str]) -> None:
        super().__init__()
        self.actions = actions

    def on(self, event: str, f=None):
        self.actions.append(f"listen:{event}")
        return super().on(event, f)


class RecordingStorage:
    def __init__(self, root: Path, actions: list[str]) -> None:
        self.root = root
        self.actions = actions
        actions.append("storage")
        self._storage = ArtifactStorage(root)

    def clear_orphans(self) -> None:
        self.actions.append("orphans")
        self._storage.clear_orphans()

    def allocate_download_directory(self, job_id: UUID, artifact_id: UUID) -> Path:
        return self._storage.allocate_download_directory(job_id, artifact_id)


class RecordingReadiness:
    def __init__(self, actions: list[str]) -> None:
        self.accepting = False
        self.actions = actions

    def open(self) -> None:
        self.accepting = True
        self.actions.append("ready:true")

    def close(self) -> None:
        self.accepting = False
        self.actions.append("ready:false")

    def is_accepting(self) -> bool:
        return self.accepting


class RecordingTracker:
    def __init__(self, actions: list[str]) -> None:
        self.actions = actions

    def register(self, bus: RecordingBus) -> None:
        self.actions.append("tracker")
        bus.on(JOB_QUEUED, lambda _event: None)

    def get_job(self, _job_id):
        return None

    def get_batch(self, _batch_id):
        return None


class RecordingScheduler:
    accepting = True

    def __init__(self, bus: RecordingBus, _storage: object, actions: list[str]) -> None:
        self.actions = actions
        self.stopped = False
        self.failed_with = None
        actions.append("scheduler")
        bus.on(TELEGRAM_UNAVAILABLE, lambda _event: None)

    def stop(self) -> None:
        self.stopped = True
        self.accepting = False
        self.actions.append("scheduler:stop")

    def fail(self, error: object) -> None:
        self.failed_with = error
        self.stop()


class RecordingProducer:
    def __init__(self, bus: RecordingBus, actions: list[str]) -> None:
        actions.append("producer")
        bus.on(YOUTUBE_DOWNLOAD_REQUESTED, lambda _event: None)


class RecordingTelegram:
    def __init__(self, actions: list[str]) -> None:
        self.actions = actions
        self.connected = False

    def is_connected(self) -> bool:
        return self.connected

    async def disconnect(self) -> None:
        self.connected = False
        self.actions.append("disconnect")


class RecordingUploader:
    def __init__(
        self,
        bus: RecordingBus,
        telegram: RecordingTelegram,
        actions: list[str],
        *,
        fail_start: bool,
    ) -> None:
        self.telegram = telegram
        self.actions = actions
        self.fail_start = fail_start
        self.stopped = False
        actions.append("uploader")
        bus.on(ARTIFACT_READY, lambda _event: None)

    async def start(self) -> None:
        self.actions.append("connect")
        self.telegram.connected = True
        if self.fail_start:
            raise RuntimeError("safe startup failure")
        self.actions.append("monitor:start")

    def begin_shutdown(self) -> None:
        pass

    async def stop(self, *, disconnect: bool = True) -> None:
        self.stopped = True
        self.actions.append("uploader:stop")
        self.actions.append("monitor:stop")
        if disconnect:
            await self.telegram.disconnect()


def recording_adapters(
    actions: list[str], *, fail_start: bool = False
) -> AdapterFactories:
    return AdapterFactories(
        bus_factory=lambda: RecordingBus(actions),
        storage_factory=lambda settings: RecordingStorage(
            settings.artifact_root, actions
        ),
        readiness_factory=lambda: RecordingReadiness(actions),
        tracker_factory=lambda: RecordingTracker(actions),
        scheduler_factory=lambda bus, storage: RecordingScheduler(
            bus, storage, actions
        ),
        process_runner_factory=lambda: object(),
        youtube_factory=lambda bus, _storage, _runner, _settings: RecordingProducer(
            bus, actions
        ),
        telegram_factory=lambda _settings: RecordingTelegram(actions),
        uploader_factory=lambda bus, _storage, telegram, _settings: RecordingUploader(
            bus, telegram, actions, fail_start=fail_start
        ),
    )


async def test_lifespan_startup_is_lazy_and_orders_readiness(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    producer_kwargs = {}

    def capture_producer(*args, **kwargs):
        producer_kwargs.update(kwargs)
        return object()

    monkeypatch.setattr(main_module, "YouTubeArtifactProducer", capture_producer)
    settings = make_settings(tmp_path / "wiring")
    main_module._youtube(object(), object(), object(), settings)
    assert producer_kwargs["max_artifact_bytes"] == settings.max_artifact_bytes

    config_bases: list[Path] = []

    def load_config(_cls, base_dir: Path) -> Settings:
        config_bases.append(base_dir)
        return make_settings(tmp_path / "loaded")

    monkeypatch.setattr(Settings, "from_env", classmethod(load_config))
    lazy_actions: list[str] = []
    lazy_app = create_app(adapters=recording_adapters(lazy_actions))
    assert config_bases == []
    assert not hasattr(lazy_app.state, "bus")
    caller_dir = tmp_path / "elsewhere"
    caller_dir.mkdir()
    monkeypatch.chdir(caller_dir)
    async with lazy_app.router.lifespan_context(lazy_app):
        assert lazy_app.state.readiness.is_accepting() is True
    assert config_bases == [Path(main_module.__file__).resolve().parent.parent]
    assert config_bases[0] != caller_dir

    actions: list[str] = []
    service = create_app(make_settings(tmp_path), recording_adapters(actions))
    async with service.router.lifespan_context(service):
        assert service.state.readiness.is_accepting() is True
        assert service.state.telegram.is_connected() is True
        assert actions.count(f"listen:{ERROR}") == 1
        assert actions.index("orphans") < actions.index(f"listen:{ERROR}")
        assert actions.index(f"listen:{ARTIFACT_READY}") < actions.index("connect")
        assert actions.index("connect") < actions.index("monitor:start")
        assert actions.index("monitor:start") < actions.index("ready:true")

    failed_actions: list[str] = []
    failed = create_app(
        make_settings(tmp_path / "failed"),
        recording_adapters(failed_actions, fail_start=True),
    )
    with pytest.raises(RuntimeError, match="safe startup failure"):
        async with failed.router.lifespan_context(failed):
            raise AssertionError("startup unexpectedly completed")
    assert "ready:true" not in failed_actions
    assert failed_actions[-4:] == [
        "uploader:stop",
        "monitor:stop",
        "orphans",
        "disconnect",
    ]
    assert not hasattr(failed.state, "bus")


async def test_lifespan_shutdown_stops_admission_drains_and_disconnects_last(
    tmp_path: Path,
) -> None:
    actions: list[str] = []
    service = create_app(
        make_settings(tmp_path, grace=0.001), recording_adapters(actions)
    )
    started = asyncio.Event()
    cancelled = asyncio.Event()
    release = asyncio.Event()
    job_id = UUID("10000000-0000-0000-0000-000000000001")
    artifact_id = UUID("30000000-0000-0000-0000-000000000001")

    async with service.router.lifespan_context(service):
        bus = service.state.bus

        async def active_work(_event: object) -> None:
            actions.append("active:start")
            started.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                actions.append("active:cancelled")
                cancelled.set()
                raise

        bus.on("active.work", active_work)
        staged_dir = service.state.storage.allocate_download_directory(
            job_id, artifact_id
        )
        staged_file = staged_dir / "queued.mp4"
        staged_file.write_bytes(b"queued")
        bus.emit("active.work", object())
        await started.wait()
        shutdown_at = len(actions)

    shutdown = actions[shutdown_at:]
    assert shutdown[:2] == ["ready:false", "scheduler:stop"]
    assert cancelled.is_set()
    assert shutdown.index("active:cancelled") < shutdown.index("uploader:stop")
    assert shutdown.index("monitor:stop") < shutdown.index("orphans")
    assert shutdown[-1] == "disconnect"
    assert staged_file.exists() is False
    assert bus.complete is True
    assert not hasattr(service.state, "scheduler")
