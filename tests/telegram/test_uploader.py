import asyncio
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from pyee.asyncio import AsyncIOEventEmitter

from anything2telegram.artifacts.storage import ArtifactStorage
from anything2telegram.config import Settings
from anything2telegram.events import (
    ARTIFACT_UPLOAD_FAILED,
    ARTIFACT_UPLOADED,
    ERROR,
    TELEGRAM_UNAVAILABLE,
    ArtifactReady,
    DownloadTarget,
    PlaylistExpanded,
)
from anything2telegram.telegram.client import (
    TelegramClientError,
    TelegramUnavailableError,
    TelegramUploadError,
)
from anything2telegram.telegram.uploader import TelegramArtifactUploader
from tests.conftest import FakeTelegram, settings_for


class Recorder:
    def __init__(self, bus: AsyncIOEventEmitter) -> None:
        self.uploaded: list[object] = []
        self.failed: list[object] = []
        self.unavailable: list[object] = []
        self.errors: list[object] = []
        bus.on(ARTIFACT_UPLOADED, self.uploaded.append)
        bus.on(ARTIFACT_UPLOAD_FAILED, self.failed.append)
        bus.on(TELEGRAM_UNAVAILABLE, self.unavailable.append)
        bus.on(ERROR, self.errors.append)

    def failure_codes(self) -> list[str]:
        return [event.error.code for event in self.failed]


@pytest.fixture
def bus() -> AsyncIOEventEmitter:
    return AsyncIOEventEmitter()


@pytest.fixture
def recorder(bus: AsyncIOEventEmitter) -> Recorder:
    return Recorder(bus)


@pytest.fixture
def storage(tmp_path: Path) -> ArtifactStorage:
    return ArtifactStorage(tmp_path / "artifacts")


@pytest.fixture
def telegram() -> FakeTelegram:
    return FakeTelegram()


@pytest.fixture
async def uploader(
    bus: AsyncIOEventEmitter,
    storage: ArtifactStorage,
    telegram: FakeTelegram,
    settings: Settings,
):
    instance = TelegramArtifactUploader(bus, storage, telegram, settings)
    await instance.start()
    yield instance
    await instance.stop()


def ready_artifact(
    storage: ArtifactStorage, *, payload: bytes = b"payload", caption: str | None = "hi"
) -> ArtifactReady:
    job_id, artifact_id = uuid4(), uuid4()
    directory = storage.allocate_download_directory(job_id, artifact_id)
    path = directory / "clip.mp4"
    path.write_bytes(payload)
    return ArtifactReady(
        job_id,
        artifact_id,
        path,
        "clip.mp4",
        "video/mp4",
        len(payload),
        caption,
        datetime.now(UTC),
    )


async def settle(bus: AsyncIOEventEmitter) -> None:
    while not bus.complete:
        await asyncio.wait_for(bus.wait_for_complete(), timeout=2)


async def test_an_expanded_playlist_posts_its_name_as_a_header(
    uploader, bus, telegram, recorder
) -> None:
    targets = (DownloadTarget("a" * 11, "https://youtu.be/aaaaaaaaaaa"),)

    bus.emit(
        "youtube.playlist.expanded",
        PlaylistExpanded(uuid4(), targets, 0, datetime.now(UTC), "Rust Fundamentals"),
    )
    bus.emit(
        "youtube.playlist.expanded",
        PlaylistExpanded(uuid4(), targets, 0, datetime.now(UTC), None),
    )
    await settle(bus)

    assert telegram.messages == ["Rust Fundamentals - 1 video(s)"]
    assert recorder.errors == []


async def test_a_failed_header_does_not_fail_the_batch(
    uploader, bus, telegram, storage, recorder, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def boom(_text: str) -> None:
        raise TelegramUploadError()

    monkeypatch.setattr(telegram, "send_message", boom)

    bus.emit(
        "youtube.playlist.expanded",
        PlaylistExpanded(uuid4(), (), 0, datetime.now(UTC), "Rust Fundamentals"),
    )
    bus.emit("artifact.ready", ready_artifact(storage))
    await settle(bus)

    assert recorder.errors == []
    assert recorder.failure_codes() == []
    assert len(recorder.uploaded) == 1


async def test_a_ready_artifact_is_uploaded_and_reported(
    uploader, bus, storage, telegram, recorder
) -> None:
    event = ready_artifact(storage)

    bus.emit("artifact.ready", event)
    await settle(bus)

    assert telegram.uploaded_filenames == ["clip.mp4"]
    assert telegram.captions == ["hi"]
    assert recorder.failure_codes() == []
    assert len(recorder.uploaded) == 1
    uploaded = recorder.uploaded[0]
    assert (uploaded.job_id, uploaded.artifact_id) == (event.job_id, event.artifact_id)
    assert (uploaded.telegram_chat_id, uploaded.telegram_message_id) == (-1001, 1)


async def test_the_same_artifact_is_never_uploaded_twice(
    uploader, bus, storage, telegram, recorder
) -> None:
    event = ready_artifact(storage)

    bus.emit("artifact.ready", event)
    await settle(bus)
    bus.emit("artifact.ready", event)
    await settle(bus)

    assert telegram.uploaded_filenames == ["clip.mp4"]
    assert len(recorder.uploaded) == 1


async def test_distinct_artifacts_each_get_their_own_outcome(
    uploader, bus, storage, telegram, recorder
) -> None:
    first = ready_artifact(storage)
    second = ready_artifact(storage)

    bus.emit("artifact.ready", first)
    bus.emit("artifact.ready", second)
    await settle(bus)

    assert len(telegram.uploaded_filenames) == 2
    assert {event.job_id for event in recorder.uploaded} == {
        first.job_id,
        second.job_id,
    }


async def test_a_missing_artifact_fails_before_contacting_telegram(
    uploader, bus, storage, telegram, recorder
) -> None:
    event = ready_artifact(storage)
    event.local_path.unlink()

    bus.emit("artifact.ready", event)
    await settle(bus)

    assert recorder.failure_codes() == ["artifact_missing"]
    assert telegram.uploaded_filenames == []


async def test_an_artifact_that_changed_size_is_rejected_as_drift(
    uploader, bus, storage, telegram, recorder
) -> None:
    event = ready_artifact(storage)
    event.local_path.write_bytes(b"a different payload entirely")

    bus.emit("artifact.ready", event)
    await settle(bus)

    assert recorder.failure_codes() == ["artifact_drift"]
    assert telegram.uploaded_filenames == []


async def test_an_artifact_outside_managed_storage_is_rejected(
    uploader, bus, storage, telegram, recorder, tmp_path
) -> None:
    outside = tmp_path / "elsewhere.mp4"
    outside.write_bytes(b"payload")
    event = ArtifactReady(
        uuid4(),
        uuid4(),
        outside,
        "elsewhere.mp4",
        "video/mp4",
        7,
        None,
        datetime.now(UTC),
    )

    bus.emit("artifact.ready", event)
    await settle(bus)

    assert recorder.failure_codes() == ["artifact_outside"]
    assert telegram.uploaded_filenames == []


async def test_an_artifact_over_the_configured_limit_is_rejected(
    bus, storage, telegram, recorder, tmp_path
) -> None:
    small = settings_for(tmp_path, max_artifact_bytes=4)
    uploader = TelegramArtifactUploader(bus, storage, telegram, small)
    await uploader.start()
    try:
        bus.emit("artifact.ready", ready_artifact(storage, payload=b"much too long"))
        await settle(bus)
    finally:
        await uploader.stop()

    assert recorder.failure_codes() == ["artifact_oversize"]
    assert telegram.uploaded_filenames == []


@pytest.mark.parametrize(
    "error,code",
    [
        (TelegramUploadError(), "telegram_upload_failed"),
        (RuntimeError("boom"), "internal_error"),
    ],
)
async def test_upload_failures_map_to_stable_codes(
    uploader, bus, storage, telegram, recorder, error: BaseException, code: str
) -> None:
    telegram.upload_errors = [error]

    bus.emit("artifact.ready", ready_artifact(storage))
    await settle(bus)

    assert recorder.failure_codes() == [code]
    assert recorder.uploaded == []


async def test_a_timeout_fails_the_job_without_stopping_the_uploader(
    bus, storage, recorder, tmp_path
) -> None:
    telegram = FakeTelegram(manual_release=True)
    impatient = settings_for(tmp_path, tg_upload_timeout_seconds=0.01)
    uploader = TelegramArtifactUploader(bus, storage, telegram, impatient)
    await uploader.start()
    try:
        bus.emit("artifact.ready", ready_artifact(storage))
        await settle(bus)
    finally:
        telegram.releases[0].set()
        await uploader.stop()

    assert recorder.failure_codes() == ["telegram_timeout"]


async def test_losing_telegram_fails_the_job_and_announces_unavailability(
    uploader, bus, storage, telegram, recorder
) -> None:
    telegram.upload_errors = [TelegramUnavailableError()]

    bus.emit("artifact.ready", ready_artifact(storage))
    await settle(bus)

    assert recorder.failure_codes() == ["telegram_unavailable"]
    assert len(recorder.unavailable) == 1


async def test_unavailability_is_announced_only_once(
    uploader, bus, storage, telegram, recorder
) -> None:
    telegram.upload_errors = [TelegramUnavailableError(), TelegramUnavailableError()]

    bus.emit("artifact.ready", ready_artifact(storage))
    await settle(bus)
    bus.emit("artifact.ready", ready_artifact(storage))
    await settle(bus)

    assert len(recorder.unavailable) == 1


async def test_the_pause_signal_waits_for_the_in_flight_upload_to_settle(
    bus, storage, settings, recorder
) -> None:
    telegram = FakeTelegram(manual_release=True)
    uploader = TelegramArtifactUploader(bus, storage, telegram, settings)
    await uploader.start()
    try:
        bus.emit("artifact.ready", ready_artifact(storage))
        await asyncio.wait_for(telegram.started[0].wait(), timeout=1)

        # The connection drops while the upload is still running.
        telegram.drop()
        await asyncio.sleep(0)
        assert recorder.unavailable == []

        telegram.releases[0].set()
        await settle(bus)
    finally:
        await uploader.stop()

    assert len(recorder.uploaded) == 1
    assert len(recorder.unavailable) == 1


async def test_a_disconnect_while_idle_announces_unavailability(
    uploader, bus, telegram, recorder
) -> None:
    telegram.drop()
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert len(recorder.unavailable) == 1


async def test_a_monitor_crash_is_published_on_the_error_topic(
    bus, storage, settings, recorder
) -> None:
    class BrokenMonitor(FakeTelegram):
        async def wait_until_disconnected(self) -> None:
            raise RuntimeError("monitor exploded")

    uploader = TelegramArtifactUploader(bus, storage, BrokenMonitor(), settings)
    await uploader.start()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    await uploader.stop()

    assert [type(error) for error in recorder.errors] == [RuntimeError]


async def test_a_client_monitor_failure_is_not_an_application_error(
    bus, storage, settings, recorder
) -> None:
    class DroppingMonitor(FakeTelegram):
        async def wait_until_disconnected(self) -> None:
            raise TelegramClientError("monitor lost")

    uploader = TelegramArtifactUploader(bus, storage, DroppingMonitor(), settings)
    await uploader.start()
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    await uploader.stop()

    assert recorder.errors == []
    assert len(recorder.unavailable) == 1


async def test_a_failed_connect_rolls_back_and_propagates(
    bus, storage, settings
) -> None:
    telegram = FakeTelegram()
    telegram.connect_error = TelegramUnavailableError()
    uploader = TelegramArtifactUploader(bus, storage, telegram, settings)

    with pytest.raises(TelegramUnavailableError):
        await uploader.start()
    assert telegram.disconnected.is_set()


async def test_begin_shutdown_refuses_new_artifacts_but_keeps_the_connection(
    uploader, bus, storage, telegram, recorder
) -> None:
    uploader.begin_shutdown()

    bus.emit("artifact.ready", ready_artifact(storage))
    await settle(bus)

    assert telegram.uploaded_filenames == []
    assert recorder.uploaded == []
    assert recorder.failed == []
    assert telegram.connected is True


async def test_stop_can_leave_the_connection_open_for_a_later_disconnect(
    bus, storage, telegram, settings
) -> None:
    uploader = TelegramArtifactUploader(bus, storage, telegram, settings)
    await uploader.start()

    await uploader.stop(disconnect=False)

    assert telegram.connected is True


async def test_stop_cancels_an_upload_that_is_still_running(
    bus, storage, settings, recorder
) -> None:
    telegram = FakeTelegram(manual_release=True)
    uploader = TelegramArtifactUploader(bus, storage, telegram, settings)
    await uploader.start()
    bus.emit("artifact.ready", ready_artifact(storage))
    await asyncio.wait_for(telegram.started[0].wait(), timeout=1)

    await uploader.stop()

    assert recorder.uploaded == []
    assert telegram.connected is False


async def test_a_failed_upload_logs_the_job_and_the_reason(
    uploader, bus, storage, telegram, recorder, caplog
) -> None:
    telegram.upload_errors = [TelegramUploadError()]
    event = ready_artifact(storage)

    with caplog.at_level("WARNING", logger="anything2telegram"):
        bus.emit("artifact.ready", event)
        await settle(bus)

    assert recorder.failure_codes() == ["telegram_upload_failed"]
    logged = caplog.text
    assert str(event.job_id) in logged
    assert str(event.artifact_id) in logged
    assert "telegram_upload_failed" in logged
    assert "clip.mp4" in logged


async def test_an_unexpected_upload_crash_logs_a_traceback(
    uploader, bus, storage, telegram, recorder, caplog
) -> None:
    telegram.upload_errors = [RuntimeError("client exploded")]

    with caplog.at_level("WARNING", logger="anything2telegram"):
        bus.emit("artifact.ready", ready_artifact(storage))
        await settle(bus)

    assert recorder.failure_codes() == ["internal_error"]
    assert "client exploded" in caplog.text
    assert "Traceback" in caplog.text


async def test_a_validation_rejection_logs_without_a_bogus_traceback(
    uploader, bus, storage, recorder, caplog
) -> None:
    event = ready_artifact(storage)
    event.local_path.unlink()

    with caplog.at_level("WARNING", logger="anything2telegram"):
        bus.emit("artifact.ready", event)
        await settle(bus)

    assert recorder.failure_codes() == ["artifact_missing"]
    assert "artifact_missing" in caplog.text
    assert "NoneType: None" not in caplog.text


async def test_losing_telegram_says_so_at_error_level(
    uploader, bus, storage, telegram, recorder, caplog
) -> None:
    telegram.upload_errors = [TelegramUnavailableError()]

    with caplog.at_level("WARNING", logger="anything2telegram"):
        bus.emit("artifact.ready", ready_artifact(storage))
        await settle(bus)

    assert "Telegram is unavailable" in caplog.text
    assert [r.levelname for r in caplog.records if "pausing" in r.message] == ["ERROR"]
