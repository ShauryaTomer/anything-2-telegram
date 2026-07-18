import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from anything2telegram.telegram.client import (
    TelegramClientAdapter,
    TelegramUnavailableError,
)


class FakeTelethonClient:
    def __init__(self, session: str, api_id: int, api_hash: str) -> None:
        self.constructor_args = (session, api_id, api_hash)
        self.is_connected = False
        self.started_with: str | None = None
        self.sent: tuple[object, ...] | None = None
        self.disconnected = asyncio.Event()
        self.upload_error: Exception | None = None

    async def start(self, *, bot_token: str) -> None:
        self.started_with = bot_token
        self.is_connected = True

    async def disconnect(self) -> None:
        self.is_connected = False
        self.disconnected.set()

    async def run_until_disconnected(self) -> None:
        await self.disconnected.wait()

    async def send_file(self, entity: int, file: str, **kwargs: object) -> object:
        if self.upload_error is not None:
            raise self.upload_error
        self.sent = (entity, file, kwargs)
        return SimpleNamespace(chat_id=entity, id=73)


async def test_adapter_wires_telethon_and_maps_connection_errors_safely(
    tmp_path: Path,
) -> None:
    created: list[FakeTelethonClient] = []

    def factory(session: str, api_id: int, api_hash: str) -> FakeTelethonClient:
        client = FakeTelethonClient(session, api_id, api_hash)
        created.append(client)
        return client

    settings = SimpleNamespace(
        api_id=42,
        api_hash="secret-hash",
        bot_token="secret-token",
        channel_id=-100123,
        session_path=tmp_path / "private.session",
    )
    adapter = TelegramClientAdapter(settings, client_factory=factory)

    assert "secret" not in repr(adapter)
    assert str(settings.session_path) not in repr(adapter)
    await adapter.connect()
    assert adapter.is_connected
    assert created[0].constructor_args == (
        str(settings.session_path),
        42,
        "secret-hash",
    )
    assert created[0].started_with == "secret-token"

    progress = lambda sent, total: None
    result = await adapter.upload(
        tmp_path / "clip.mp4",
        caption="caption",
        supports_streaming=True,
        progress_callback=progress,
    )
    assert (result.chat_id, result.message_id) == (-100123, 73)
    assert created[0].sent == (
        -100123,
        str(tmp_path / "clip.mp4"),
        {
            "caption": "caption",
            "supports_streaming": True,
            "progress_callback": progress,
        },
    )

    created[0].upload_error = ConnectionError("phone + destination leaked")
    with pytest.raises(TelegramUnavailableError) as raised:
        await adapter.upload(
            tmp_path / "clip.mp4",
            caption=None,
            supports_streaming=True,
            progress_callback=progress,
        )
    assert str(raised.value) == "Telegram is unavailable"
    assert "phone" not in repr(raised.value)

    await adapter.disconnect()
    await adapter.wait_until_disconnected()
    assert not adapter.is_connected
