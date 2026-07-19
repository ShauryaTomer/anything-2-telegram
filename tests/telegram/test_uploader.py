import asyncio
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest
from pyee.asyncio import AsyncIOEventEmitter

from anything2telegram.artifacts.storage import ArtifactStorage
from anything2telegram.domain import TelegramUploadResult
from anything2telegram.events import (
    ARTIFACT_READY,
    ARTIFACT_UPLOAD_FAILED,
    ARTIFACT_UPLOADED,
    TELEGRAM_UNAVAILABLE,
    ArtifactReady,
)
from anything2telegram.telegram.client import TelegramUnavailableError
from anything2telegram.telegram.uploader import TelegramArtifactUploader


NOW = datetime(2026, 7, 19, 10, 0, tzinfo=UTC)
JOB = UUID("10000000-0000-0000-0000-000000000001")
ARTIFACT = UUID("30000000-0000-0000-0000-000000000001")


class FakeClient:
    def __init__(self) -> None:
        self.is_connected = False
        self.uploads: list[dict[str, object]] = []
        self.upload_error: BaseException | None = None
        self.block_upload = False
        self.disconnected = asyncio.Event()
        self.release_upload = asyncio.Event()
        self.upload_started = asyncio.Event()
        self.disconnect_calls = 0
        self.connect_calls = 0
        self.actions: list[str] = []
        self.connect_error: BaseException | None = None
        self.disconnect_error: BaseException | None = None

    async def connect(self) -> None:
        self.connect_calls += 1
        self.is_connected = True
        if self.connect_error is not None:
            raise self.connect_error

    async def disconnect(self) -> None:
        self.disconnect_calls += 1
        self.actions.append("disconnect")
        self.is_connected = False
        self.disconnected.set()
        if self.disconnect_error is not None:
            raise self.disconnect_error

    async def wait_until_disconnected(self) -> None:
        await self.disconnected.wait()

    async def upload(self, path: Path, **kwargs: object) -> TelegramUploadResult:
        self.uploads.append({"path": path, **kwargs})
        self.upload_started.set()
        callback = kwargs["progress_callback"]
        assert callable(callback)
        callback(1, 10)
        callback(10, 10)
        if self.block_upload:
            try:
                await self.release_upload.wait()
            except asyncio.CancelledError:
                self.actions.append("upload_cancelled")
                raise
        if self.upload_error is not None:
            raise self.upload_error
        return TelegramUploadResult(-100123, 91)


def settings(*, max_bytes: int = 100, timeout: float = 1) -> object:
    return SimpleNamespace(
        max_artifact_bytes=max_bytes,
        tg_upload_timeout_seconds=timeout,
    )


def staged_event(
    storage: ArtifactStorage,
    *,
    artifact_id: UUID = ARTIFACT,
    content: bytes = b"video",
    media_type: str | None = "video/mp4",
    caption: str | None = "hello",
) -> ArtifactReady:
    directory = storage.allocate_download_directory(JOB, artifact_id)
    path = directory / "clip.mp4"
    path.write_bytes(content)
    return ArtifactReady(
        JOB,
        artifact_id,
        path,
        path.name,
        media_type,
        len(content),
        caption,
        NOW,
    )


def capture(bus: AsyncIOEventEmitter) -> list[tuple[str, object]]:
    facts: list[tuple[str, object]] = []
    for topic in (ARTIFACT_UPLOADED, ARTIFACT_UPLOAD_FAILED, TELEGRAM_UNAVAILABLE):
        bus.on(topic, lambda event, topic=topic: facts.append((topic, event)))
    return facts


async def settle_until(predicate: object) -> None:
    for _ in range(20):
        if predicate():
            return
        await asyncio.sleep(0)
    assert predicate()


async def test_upload_success_preserves_caption_video_flag_and_result(
    tmp_path: Path,
) -> None:
    bus = AsyncIOEventEmitter()
    storage = ArtifactStorage(tmp_path / "artifacts")
    client = FakeClient()
    facts = capture(bus)
    uploader = TelegramArtifactUploader(
        bus, storage, client, settings(), clock=lambda: NOW
    )
    event = staged_event(storage)

    await uploader.handle_artifact_ready(event)

    assert len(client.uploads) == 1
    assert client.uploads[0]["path"] == event.local_path
    assert client.uploads[0]["caption"] == "hello"
    assert client.uploads[0]["supports_streaming"] is True
    assert len(facts) == 1
    topic, uploaded = facts[0]
    assert topic == ARTIFACT_UPLOADED
    assert uploaded.job_id == JOB
    assert uploaded.artifact_id == ARTIFACT
    assert uploaded.telegram_chat_id == -100123
    assert uploaded.telegram_message_id == 91


@pytest.mark.parametrize(
    ("case", "expected_code"),
    [
        ("missing", "artifact_missing"),
        ("outside", "artifact_outside"),
        ("oversize", "artifact_oversize"),
    ],
)
async def test_invalid_artifacts_fail_before_client_call(
    tmp_path: Path, case: str, expected_code: str
) -> None:
    bus = AsyncIOEventEmitter()
    storage = ArtifactStorage(tmp_path / "artifacts")
    client = FakeClient()
    facts = capture(bus)
    uploader = TelegramArtifactUploader(
        bus, storage, client, settings(max_bytes=4), clock=lambda: NOW
    )
    if case == "missing":
        event = ArtifactReady(
            JOB,
            ARTIFACT,
            storage.root / str(JOB) / str(ARTIFACT) / "clip.mp4",
            "clip.mp4",
            "video/mp4",
            5,
            None,
            NOW,
        )
    elif case == "outside":
        path = tmp_path / "outside.mp4"
        path.write_bytes(b"data")
        event = ArtifactReady(
            JOB, ARTIFACT, path, path.name, "video/mp4", 4, None, NOW
        )
    else:
        event = staged_event(storage, content=b"large")

    await uploader.handle_artifact_ready(event)

    assert client.uploads == []
    assert [topic for topic, _ in facts] == [ARTIFACT_UPLOAD_FAILED]
    assert facts[0][1].error.code == expected_code


@pytest.mark.parametrize(
    ("error", "block_upload", "expected_code"),
    [
        (None, True, "telegram_timeout"),
        (RuntimeError("private data"), False, "internal_error"),
    ],
)
async def test_upload_errors_map_to_one_safe_failure(
    tmp_path: Path,
    error: BaseException | None,
    block_upload: bool,
    expected_code: str,
) -> None:
    bus = AsyncIOEventEmitter()
    storage = ArtifactStorage(tmp_path / "artifacts")
    client = FakeClient()
    client.upload_error = error
    client.block_upload = block_upload
    facts = capture(bus)
    uploader = TelegramArtifactUploader(
        bus,
        storage,
        client,
        settings(timeout=0.01),
        clock=lambda: NOW,
    )

    await uploader.handle_artifact_ready(staged_event(storage))

    assert [topic for topic, _ in facts] == [ARTIFACT_UPLOAD_FAILED]
    assert facts[0][1].error.code == expected_code
    assert "private" not in facts[0][1].error.message


async def test_connection_failure_orders_failure_then_one_unavailable(
    tmp_path: Path,
) -> None:
    bus = AsyncIOEventEmitter()
    storage = ArtifactStorage(tmp_path / "artifacts")
    client = FakeClient()
    client.upload_error = TelegramUnavailableError()
    facts = capture(bus)
    uploader = TelegramArtifactUploader(
        bus, storage, client, settings(), clock=lambda: NOW
    )
    first = staged_event(storage)
    second = staged_event(
        storage,
        artifact_id=UUID("30000000-0000-0000-0000-000000000002"),
    )

    await uploader.handle_artifact_ready(first)
    await uploader.handle_artifact_ready(second)

    assert [topic for topic, _ in facts] == [
        ARTIFACT_UPLOAD_FAILED,
        TELEGRAM_UNAVAILABLE,
        ARTIFACT_UPLOAD_FAILED,
    ]
    assert facts[0][1].error.code == "telegram_unavailable"


async def test_idle_disconnect_emits_once_but_shutdown_disconnect_is_suppressed(
    tmp_path: Path,
) -> None:
    bus = AsyncIOEventEmitter()
    storage = ArtifactStorage(tmp_path / "artifacts")
    client = FakeClient()
    facts = capture(bus)
    uploader = TelegramArtifactUploader(bus, storage, client, settings())

    await uploader.start()
    client.disconnected.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert [topic for topic, _ in facts] == [TELEGRAM_UNAVAILABLE]
    await uploader.stop()

    shutdown_bus = AsyncIOEventEmitter()
    shutdown_client = FakeClient()
    shutdown_facts = capture(shutdown_bus)
    shutdown = TelegramArtifactUploader(
        shutdown_bus, storage, shutdown_client, settings()
    )
    await shutdown.start()
    await shutdown.stop()
    await asyncio.sleep(0)
    assert shutdown_facts == []


async def test_duplicate_artifact_event_uploads_once(tmp_path: Path) -> None:
    bus = AsyncIOEventEmitter()
    storage = ArtifactStorage(tmp_path / "artifacts")
    client = FakeClient()
    uploader = TelegramArtifactUploader(bus, storage, client, settings())
    event = staged_event(storage)

    await asyncio.gather(
        uploader.handle_artifact_ready(event),
        uploader.handle_artifact_ready(event),
    )

    for value in range(1, 1025):
        await uploader.handle_artifact_ready(
            staged_event(storage, artifact_id=UUID(int=value))
        )
    await uploader.handle_artifact_ready(event)

    assert len(client.uploads) == 1025


async def test_started_monitor_defers_unavailable_until_active_failure(
    tmp_path: Path,
) -> None:
    bus = AsyncIOEventEmitter()
    storage = ArtifactStorage(tmp_path / "artifacts")
    client = FakeClient()
    client.block_upload = True
    client.upload_error = TelegramUnavailableError()
    facts = capture(bus)
    uploader = TelegramArtifactUploader(bus, storage, client, settings())

    await uploader.start()
    bus.emit(ARTIFACT_READY, staged_event(storage))
    await client.upload_started.wait()
    client.disconnected.set()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    try:
        assert facts == []
        client.release_upload.set()
        await settle_until(lambda: len(facts) == 2)
        assert [topic for topic, _ in facts] == [
            ARTIFACT_UPLOAD_FAILED,
            TELEGRAM_UNAVAILABLE,
        ]
    finally:
        client.release_upload.set()
        await uploader.stop()


async def test_stop_cancels_inflight_then_disconnects_and_ignores_late_events(
    tmp_path: Path,
) -> None:
    bus = AsyncIOEventEmitter()
    storage = ArtifactStorage(tmp_path / "artifacts")
    client = FakeClient()
    client.block_upload = True
    facts = capture(bus)
    uploader = TelegramArtifactUploader(bus, storage, client, settings())
    first = staged_event(storage)
    late = staged_event(
        storage,
        artifact_id=UUID("30000000-0000-0000-0000-000000000002"),
    )

    await uploader.start()
    await uploader.start()
    bus.emit(ARTIFACT_READY, first)
    await client.upload_started.wait()
    try:
        await uploader.stop()
        bus.emit(ARTIFACT_READY, late)
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert client.connect_calls == 1
        assert client.actions == ["upload_cancelled", "disconnect"]
        assert len(client.uploads) == 1
        assert facts == []
    finally:
        client.release_upload.set()


async def test_partial_start_failure_rolls_back_and_remains_restartable(
    tmp_path: Path,
) -> None:
    bus = AsyncIOEventEmitter()
    storage = ArtifactStorage(tmp_path / "artifacts")
    client = FakeClient()
    primary = TelegramUnavailableError()
    client.connect_error = primary
    client.disconnect_error = RuntimeError("cleanup failed")
    facts = capture(bus)
    uploader = TelegramArtifactUploader(bus, storage, client, settings())

    with pytest.raises(TelegramUnavailableError) as raised:
        await uploader.start()

    assert raised.value is primary
    assert client.disconnect_calls == 1
    assert not client.is_connected
    assert facts == []
    bus.emit(ARTIFACT_READY, staged_event(storage))
    await asyncio.sleep(0)
    assert client.uploads == []
    await uploader.stop()
    assert client.disconnect_calls == 1

    client.connect_error = None
    client.disconnect_error = None
    await uploader.start()
    assert client.connect_calls == 2
    await uploader.stop()
    assert client.disconnect_calls == 2
