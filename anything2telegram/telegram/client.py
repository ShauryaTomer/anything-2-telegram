from collections.abc import Callable
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import ServerError, TimedOutError

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



# ponytail: 512KB = Telegram's max part size; fewer SaveFilePart RPCs.
# Telethon's send_file can't forward part_size_kb, so pre-upload the handle.
_PART_SIZE_KB = 512
_CONNECTION_ERRORS = (
    OSError,
    ServerError,
    TimedOutError,
)


class TelegramClientAdapter:
    def __init__(self, settings: Settings) -> None:
        self._client = TelegramClient(
            str(settings.session_path), settings.api_id, settings.api_hash
        )
        self._bot_token = settings.bot_token
        self._entity = settings.channel_id
        self._topic_id = settings.topic_id

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

    async def send_message(self, text: str) -> None:
        try:
            await self._client.send_message(
                self._entity, text, reply_to=self._topic_id
            )
        except _CONNECTION_ERRORS:
            raise TelegramUnavailableError() from None
        except Exception:
            raise TelegramUploadError() from None

    async def upload(
        self,
        path: Path,
        *,
        caption: str | None,
        supports_streaming: bool,
        progress_callback: Callable[[int, int], object],
        file_size: int | None = None,
    ) -> TelegramUploadResult:
        try:
            handle = await self._client.upload_file(
                str(path),
                part_size_kb=_PART_SIZE_KB,
                file_size=file_size,
                progress_callback=progress_callback,
            )
            message = await self._client.send_file(
                self._entity,
                handle,
                caption=caption,
                supports_streaming=supports_streaming,
                reply_to=self._topic_id,
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
