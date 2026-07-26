import asyncio
from dataclasses import replace
from pathlib import Path

import pytest
from telethon.errors import RpcCallFailError, ServerError, TimedOutError

from anything2telegram.config import Settings
from anything2telegram.telegram import client as client_module
from anything2telegram.telegram.client import (
    TelegramClientAdapter,
    TelegramClientError,
    TelegramUnavailableError,
    TelegramUploadError,
)


class FakeTelethon:
    def __init__(self, session: str, api_id: int, api_hash: str) -> None:
        self.init_args = (session, api_id, api_hash)
        self.is_connected = False
        self.start_error: BaseException | None = None
        self.upload_error: BaseException | None = None
        self.upload_calls: list[dict[str, object]] = []
        self.send_calls: list[dict[str, object]] = []
        self.disconnect_waiter = asyncio.Event()

    async def start(self, *, bot_token: str) -> object:
        if self.start_error is not None:
            raise self.start_error
        self.bot_token = bot_token
        self.is_connected = True
        return self

    async def disconnect(self) -> object:
        self.is_connected = False
        return None

    async def run_until_disconnected(self) -> object:
        await self.disconnect_waiter.wait()
        return None

    async def upload_file(self, file: str, **kwargs: object) -> object:
        self.upload_calls.append({"file": file, **kwargs})
        if self.upload_error is not None:
            raise self.upload_error
        return "handle"

    async def send_file(self, entity: int, file: object, **kwargs: object) -> object:
        self.send_calls.append({"entity": entity, "file": file, **kwargs})

        class Message:
            chat_id = -1001
            id = 42

        return Message()


@pytest.fixture
def telethon(monkeypatch: pytest.MonkeyPatch) -> FakeTelethon:
    created: list[FakeTelethon] = []

    def factory(session: str, api_id: int, api_hash: str) -> FakeTelethon:
        created.append(FakeTelethon(session, api_id, api_hash))
        return created[-1]

    monkeypatch.setattr(client_module, "TelegramClient", factory)
    return created


@pytest.fixture
def adapter(telethon, settings: Settings) -> TelegramClientAdapter:
    return TelegramClientAdapter(settings)


def test_the_session_and_credentials_are_handed_to_telethon(
    telethon, settings: Settings
) -> None:
    TelegramClientAdapter(settings)

    assert telethon[0].init_args == (
        str(settings.session_path),
        settings.api_id,
        settings.api_hash,
    )


async def test_connect_starts_as_a_bot_and_reports_connectivity(
    adapter: TelegramClientAdapter, telethon, settings: Settings
) -> None:
    assert adapter.is_connected is False

    await adapter.connect()

    assert adapter.is_connected is True
    assert telethon[0].bot_token == settings.bot_token


@pytest.mark.parametrize(
    "error",
    [
        ConnectionError("down"),
        OSError("down"),
        asyncio.TimeoutError(),
        TimedOutError(None, "timed out"),
        ServerError(None, "server error"),
        RpcCallFailError(None),
    ],
)
async def test_network_failures_surface_as_unavailable(
    adapter: TelegramClientAdapter, telethon, error: BaseException
) -> None:
    telethon[0].start_error = error

    with pytest.raises(TelegramUnavailableError):
        await adapter.connect()


async def test_other_connect_failures_stay_generic_and_hide_details(
    adapter: TelegramClientAdapter, telethon
) -> None:
    telethon[0].start_error = ValueError("api_hash rejected: secret")

    with pytest.raises(TelegramClientError) as error:
        await adapter.connect()
    assert "secret" not in str(error.value)


async def test_upload_pre_uploads_with_large_parts_then_sends_to_the_channel(
    adapter: TelegramClientAdapter, telethon, settings: Settings, tmp_path: Path
) -> None:
    artifact = tmp_path / "clip.mp4"
    artifact.write_bytes(b"payload")

    def progress(_sent: int, _total: int) -> None:
        return None

    result = await adapter.upload(
        artifact,
        caption="hello",
        supports_streaming=True,
        progress_callback=progress,
        file_size=7,
    )

    assert (result.chat_id, result.message_id) == (-1001, 42)
    assert telethon[0].upload_calls == [
        {
            "file": str(artifact),
            "part_size_kb": 512,
            "file_size": 7,
            "progress_callback": progress,
        }
    ]
    assert telethon[0].send_calls == [
        {
            "entity": settings.channel_id,
            "file": "handle",
            "caption": "hello",
            "supports_streaming": True,
            "reply_to": None,
        }
    ]


async def test_a_configured_topic_is_used_as_the_upload_thread(
    telethon, settings: Settings, tmp_path: Path
) -> None:
    artifact = tmp_path / "clip.mp4"
    artifact.write_bytes(b"payload")
    adapter = TelegramClientAdapter(replace(settings, topic_id=7))

    await adapter.upload(
        artifact,
        caption=None,
        supports_streaming=False,
        progress_callback=lambda _s, _t: None,
    )

    assert telethon[0].send_calls[0]["reply_to"] == 7


async def test_an_upload_rejection_is_reported_as_an_upload_error(
    adapter: TelegramClientAdapter, telethon, tmp_path: Path
) -> None:
    artifact = tmp_path / "clip.mp4"
    artifact.write_bytes(b"payload")
    telethon[0].upload_error = ValueError("file rejected")

    with pytest.raises(TelegramUploadError):
        await adapter.upload(
            artifact,
            caption=None,
            supports_streaming=False,
            progress_callback=lambda _s, _t: None,
        )


async def test_an_upload_losing_the_connection_is_reported_as_unavailable(
    adapter: TelegramClientAdapter, telethon, tmp_path: Path
) -> None:
    artifact = tmp_path / "clip.mp4"
    artifact.write_bytes(b"payload")
    telethon[0].upload_error = ConnectionError("down")

    with pytest.raises(TelegramUnavailableError):
        await adapter.upload(
            artifact,
            caption=None,
            supports_streaming=False,
            progress_callback=lambda _s, _t: None,
        )


async def test_wait_until_disconnected_returns_when_the_client_drops(
    adapter: TelegramClientAdapter, telethon
) -> None:
    waiter = asyncio.create_task(adapter.wait_until_disconnected())
    await asyncio.sleep(0)
    telethon[0].disconnect_waiter.set()

    await asyncio.wait_for(waiter, timeout=1)


async def test_disconnect_is_idempotent_and_clears_connectivity(
    adapter: TelegramClientAdapter,
) -> None:
    await adapter.connect()
    await adapter.disconnect()
    await adapter.disconnect()

    assert adapter.is_connected is False
