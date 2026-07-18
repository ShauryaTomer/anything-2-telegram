import asyncio
from collections.abc import Callable
from pathlib import Path
from typing import Protocol

from telethon import TelegramClient
from telethon.errors import RpcCallFailError, ServerError, TimedOutError

from ..config import Settings
from ..domain import TelegramUploadResult


class TelegramClientError(Exception):
    """Safe Telegram adapter failure."""


class TelegramUnavailableError(TelegramClientError):
    def __init__(self) -> None:
        super().__init__("Telegram is unavailable")


class TelegramUploadError(TelegramClientError):
    def __init__(self) -> None:
        super().__init__("Telegram upload failed")


class _TelethonClient(Protocol):
    async def start(self, *, bot_token: str) -> object: ...

    async def disconnect(self) -> object: ...

    async def run_until_disconnected(self) -> object: ...

    async def send_file(
        self,
        entity: int,
        file: str,
        *,
        caption: str | None,
        supports_streaming: bool,
        progress_callback: Callable[[int, int], object],
    ) -> object: ...


_ClientFactory = Callable[[str, int, str], _TelethonClient]
_CONNECTION_ERRORS = (
    ConnectionError,
    OSError,
    asyncio.TimeoutError,
    RpcCallFailError,
    ServerError,
    TimedOutError,
)


class TelegramClientAdapter:
    def __init__(
        self,
        settings: Settings,
        *,
        client_factory: _ClientFactory = TelegramClient,
    ) -> None:
        self._client = client_factory(
            str(settings.session_path), settings.api_id, settings.api_hash
        )
        self._bot_token = settings.bot_token
        self._entity = settings.channel_id

    @property
    def is_connected(self) -> bool:
        state = getattr(self._client, "is_connected", False)
        return bool(state() if callable(state) else state)

    async def connect(self) -> None:
        try:
            await self._client.start(bot_token=self._bot_token)
        except _CONNECTION_ERRORS:
            raise TelegramUnavailableError() from None
        except Exception:
            raise TelegramClientError("Telegram connection failed") from None

    async def disconnect(self) -> None:
        try:
            await self._client.disconnect()
        except _CONNECTION_ERRORS:
            raise TelegramUnavailableError() from None
        except Exception:
            raise TelegramClientError("Telegram disconnect failed") from None

    async def wait_until_disconnected(self) -> None:
        try:
            await self._client.run_until_disconnected()
        except _CONNECTION_ERRORS:
            raise TelegramUnavailableError() from None
        except Exception:
            raise TelegramClientError("Telegram connection monitor failed") from None

    async def upload(
        self,
        path: Path,
        *,
        caption: str | None,
        supports_streaming: bool,
        progress_callback: Callable[[int, int], object],
    ) -> TelegramUploadResult:
        try:
            message = await self._client.send_file(
                self._entity,
                str(path),
                caption=caption,
                supports_streaming=supports_streaming,
                progress_callback=progress_callback,
            )
            chat_id = message.chat_id
            message_id = message.id
            return TelegramUploadResult(chat_id, message_id)
        except _CONNECTION_ERRORS:
            raise TelegramUnavailableError() from None
        except TelegramClientError:
            raise
        except Exception:
            raise TelegramUploadError() from None
