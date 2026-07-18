import asyncio
import logging
import mimetypes
import stat
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol
from uuid import UUID

from ..artifacts.storage import ArtifactStorage, ArtifactStorageError
from ..bus import EventBus
from ..config import Settings
from ..domain import ErrorInfo, StagedArtifact, TelegramUploadResult
from ..events import (
    ARTIFACT_READY,
    ARTIFACT_UPLOAD_FAILED,
    ARTIFACT_UPLOADED,
    ERROR,
    TELEGRAM_UNAVAILABLE,
    ArtifactReady,
    ArtifactUploaded,
    ArtifactUploadFailed,
    TelegramUnavailable,
)
from .client import (
    TelegramClientError,
    TelegramUnavailableError,
    TelegramUploadError,
)


class _TelegramClient(Protocol):
    @property
    def is_connected(self) -> bool: ...

    async def connect(self) -> None: ...

    async def disconnect(self) -> None: ...

    async def wait_until_disconnected(self) -> None: ...

    async def upload(
        self,
        path: Path,
        *,
        caption: str | None,
        supports_streaming: bool,
        progress_callback: Callable[[int, int], object],
    ) -> TelegramUploadResult: ...


_ERRORS = {
    "artifact_missing": "Artifact is unavailable",
    "artifact_outside": "Artifact is outside managed storage",
    "artifact_invalid": "Artifact is invalid",
    "artifact_drift": "Artifact changed before upload",
    "artifact_oversize": "Artifact exceeds Telegram upload limit",
    "telegram_timeout": "Telegram upload timed out",
    "telegram_unavailable": "Telegram is unavailable",
    "telegram_upload_failed": "Telegram upload failed",
    "internal_error": "Artifact upload failed",
}


class TelegramArtifactUploader:
    def __init__(
        self,
        bus: EventBus,
        storage: ArtifactStorage,
        client: _TelegramClient,
        settings: Settings,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        logger: object | None = None,
        progress_interval_seconds: float = 1.0,
    ) -> None:
        self._bus = bus
        self._storage = storage
        self._client = client
        self._max_bytes = settings.max_artifact_bytes
        self._timeout_seconds = settings.tg_upload_timeout_seconds
        self._clock = clock
        self._logger = logger if logger is not None else logging.getLogger(__name__)
        self._progress_interval = progress_interval_seconds
        self._claimed: set[tuple[UUID, UUID]] = set()
        self._terminal: set[tuple[UUID, UUID]] = set()
        self._outcome_emitted: set[tuple[UUID, UUID]] = set()
        self._unavailable_emitted = False
        self._shutting_down = False
        self._monitor_task: asyncio.Task[None] | None = None
        bus.on(ARTIFACT_READY, self.handle_artifact_ready)

    async def start(self) -> None:
        self._shutting_down = False
        self._unavailable_emitted = False
        try:
            await self._client.connect()
        except TelegramUnavailableError:
            self._emit_unavailable()
            raise
        self._monitor_task = asyncio.create_task(self._monitor_disconnect())
        self._monitor_task.add_done_callback(self._monitor_done)

    async def stop(self) -> None:
        self._shutting_down = True
        monitor = self._monitor_task
        self._monitor_task = None
        if monitor is not None and not monitor.done():
            monitor.cancel()
        try:
            await self._client.disconnect()
        except Exception:
            pass
        if monitor is not None:
            await asyncio.gather(monitor, return_exceptions=True)

    async def handle_artifact_ready(self, event: ArtifactReady) -> None:
        if not isinstance(event, ArtifactReady):
            raise TypeError("event must be ArtifactReady")
        key = (event.job_id, event.artifact_id)
        if key in self._claimed or key in self._terminal:
            return
        self._claimed.add(key)
        try:
            validation_error = self._validate(event)
            if validation_error is not None:
                self._emit_failure(event, validation_error)
                return
            try:
                result = await asyncio.wait_for(
                    self._client.upload(
                        event.local_path,
                        caption=event.caption,
                        supports_streaming=self._supports_streaming(event),
                        progress_callback=self._progress_callback(),
                    ),
                    timeout=self._timeout_seconds,
                )
            except asyncio.CancelledError:
                self._claimed.discard(key)
                await asyncio.shield(self.stop())
                raise
            except TelegramUnavailableError:
                self._emit_failure(event, "telegram_unavailable")
                self._emit_unavailable()
                return
            except (asyncio.TimeoutError, TimeoutError):
                self._emit_failure(event, "telegram_timeout")
                return
            except TelegramUploadError:
                self._emit_failure(event, "telegram_upload_failed")
                return
            except Exception:
                self._emit_failure(event, "internal_error")
                return
            uploaded = ArtifactUploaded(
                event.job_id,
                event.artifact_id,
                result.chat_id,
                result.message_id,
                self._causal_time(event.occurred_at),
            )
            self._outcome_emitted.add(key)
            self._bus.emit(ARTIFACT_UPLOADED, uploaded)
        except asyncio.CancelledError:
            raise
        except Exception:
            if key in self._outcome_emitted:
                raise
            self._emit_failure(event, "internal_error")
        finally:
            if key in self._claimed:
                self._claimed.remove(key)
                self._terminal.add(key)

    def _validate(self, event: ArtifactReady) -> str | None:
        try:
            current = event.local_path.lstat()
        except FileNotFoundError:
            return "artifact_missing"
        except OSError:
            return "artifact_invalid"
        if stat.S_ISLNK(current.st_mode) or not stat.S_ISREG(current.st_mode):
            return "artifact_invalid"
        try:
            event.local_path.resolve().relative_to(self._storage.root.resolve())
        except (OSError, ValueError):
            return "artifact_outside"
        if event.size_bytes > self._max_bytes:
            return "artifact_oversize"
        staged = StagedArtifact(
            event.job_id,
            event.artifact_id,
            event.local_path,
            event.filename,
            event.media_type,
            event.size_bytes,
            event.caption,
        )
        try:
            self._storage.validate_staged_artifact(staged)
        except ArtifactStorageError:
            return "artifact_drift"
        return None

    @staticmethod
    def _supports_streaming(event: ArtifactReady) -> bool:
        media_type = event.media_type
        if media_type is not None and media_type.partition(";")[0].strip().lower().startswith(
            "video/"
        ):
            return True
        guessed, _ = mimetypes.guess_type(event.filename)
        return guessed is not None and guessed.startswith("video/")

    def _progress_callback(self) -> Callable[[int, int], None]:
        last_reported = 0.0

        def report(sent: int, total: int) -> None:
            nonlocal last_reported
            try:
                now = asyncio.get_running_loop().time()
                complete = total > 0 and sent >= total
                if not complete and now - last_reported < self._progress_interval:
                    return
                last_reported = now
                percent = sent * 100 // total if total > 0 else 0
                self._logger.info("Telegram upload progress: %d%%", percent)
            except Exception:
                pass

        return report

    async def _monitor_disconnect(self) -> None:
        try:
            await self._client.wait_until_disconnected()
        except asyncio.CancelledError:
            raise
        except TelegramClientError:
            pass
        if not self._shutting_down:
            self._emit_unavailable()

    def _monitor_done(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        try:
            error = task.exception()
        except asyncio.CancelledError:
            return
        if error is not None:
            self._bus.emit(ERROR, error)

    def _emit_failure(self, event: ArtifactReady, code: str) -> None:
        key = (event.job_id, event.artifact_id)
        if key in self._outcome_emitted:
            return
        failure = ArtifactUploadFailed(
            event.job_id,
            event.artifact_id,
            ErrorInfo(code, _ERRORS[code]),
            self._causal_time(event.occurred_at),
        )
        self._outcome_emitted.add(key)
        self._bus.emit(
            ARTIFACT_UPLOAD_FAILED,
            failure,
        )

    def _emit_unavailable(self) -> None:
        if self._shutting_down or self._unavailable_emitted:
            return
        self._unavailable_emitted = True
        self._bus.emit(
            TELEGRAM_UNAVAILABLE,
            TelegramUnavailable(
                ErrorInfo("telegram_unavailable", _ERRORS["telegram_unavailable"]),
                self._clock(),
            ),
        )

    def _causal_time(self, occurred_at: datetime) -> datetime:
        return max(self._clock(), occurred_at)
