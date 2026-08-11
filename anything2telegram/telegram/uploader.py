"""Uploads ready artifacts to Telegram, one event handler per artifact."""

import asyncio
import logging
import mimetypes
import stat
import sys
from datetime import UTC, datetime
from uuid import UUID

from ..artifacts.storage import ArtifactStorage, ArtifactStorageError
from ..config import Settings
from ..domain import ErrorInfo, JobPhase, StagedArtifact
from ..events import (
    ARTIFACT_READY,
    ARTIFACT_UPLOAD_FAILED,
    ARTIFACT_UPLOADED,
    ERROR,
    TELEGRAM_UNAVAILABLE,
    YOUTUBE_PLAYLIST_EXPANDED,
    ArtifactReady,
    ArtifactUploaded,
    ArtifactUploadFailed,
    PlaylistExpanded,
    TelegramUnavailable,
)
from ..jobs.progress import ProgressRegistry
from ..tui import transfer
from .client import (
    TelegramClientError,
    TelegramUnavailableError,
    TelegramUploadError,
)


_LOGGER = logging.getLogger(__name__)

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
        bus,
        storage: ArtifactStorage,
        client,
        settings: Settings,
        progress_registry: ProgressRegistry,
    ) -> None:
        self._bus = bus
        self._storage = storage
        self._client = client
        self._max_bytes = settings.max_artifact_bytes
        self._timeout_seconds = settings.tg_upload_timeout_seconds
        self._progress = progress_registry
        self._handled: set[tuple[UUID, UUID]] = set()
        self._emitted: set[tuple[UUID, UUID]] = set()
        self._inflight: set[asyncio.Task[object]] = set()
        self._monitor_task: asyncio.Task[None] | None = None
        self._unavailable_emitted = False
        self._unavailable_pending = False
        self._accepting = True
        self._stopping = False
        self._shutting_down = False
        bus.on(ARTIFACT_READY, self.handle_artifact_ready)
        bus.on(YOUTUBE_PLAYLIST_EXPANDED, self.handle_playlist_expanded)

    async def start(self) -> None:
        try:
            await self._client.connect()
        except BaseException:
            await self._stop(asyncio.current_task())
            raise
        self._monitor_task = asyncio.create_task(self._monitor_disconnect())
        self._monitor_task.add_done_callback(self._monitor_done)

    async def stop(self, *, disconnect: bool = True) -> None:
        if self._stopping:
            return
        await self._stop(asyncio.current_task(), disconnect=disconnect)

    def begin_shutdown(self) -> None:
        """Stop admitting new artifacts while letting in-flight uploads finish."""
        self._shutting_down = True
        self._accepting = False

    async def _stop(
        self,
        current: asyncio.Task[object] | None,
        *,
        disconnect: bool = True,
    ) -> None:
        self._stopping = True
        self._shutting_down = True
        self._accepting = False
        self._unavailable_pending = False
        pending = [task for task in self._inflight if task is not current]
        for task in pending:
            task.cancel()
        monitor = self._monitor_task
        self._monitor_task = None
        if monitor is not None:
            monitor.cancel()
            pending.append(monitor)
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        if disconnect:
            try:
                await self._client.disconnect()
            except Exception:
                _LOGGER.exception("Telegram disconnect failed")

    async def handle_playlist_expanded(self, event: PlaylistExpanded) -> None:
        """Post the playlist name as a header above the videos that follow."""
        if not self._accepting or not event.playlist_title:
            return
        task = asyncio.current_task()
        if task is not None:
            self._inflight.add(task)
        try:
            await self._client.send_message(
                f"{event.playlist_title} - {len(event.targets)} video(s)"
            )
        except asyncio.CancelledError:
            raise
        # ponytail: the header is cosmetic, so a failed post never fails the
        # batch. Emit a job failure instead if it ever has to be delivered.
        except TelegramClientError:
            _LOGGER.warning(
                "Playlist header message failed: batch=%s", event.batch_id
            )
        except Exception:
            _LOGGER.exception(
                "Playlist header message failed: batch=%s", event.batch_id
            )
        finally:
            if task is not None:
                self._inflight.discard(task)
            self._flush_unavailable()

    async def handle_artifact_ready(self, event: ArtifactReady) -> None:
        if not self._accepting:
            return
        key = (event.job_id, event.artifact_id)
        if key in self._handled:
            return
        self._handled.add(key)
        task = asyncio.current_task()
        if task is not None:
            self._inflight.add(task)
        try:
            await self._upload(event, key)
        except asyncio.CancelledError:
            raise
        except Exception:
            self._emit_failure(event, "internal_error")
        finally:
            self._emitted.discard(key)
            if task is not None:
                self._inflight.discard(task)
            self._flush_unavailable()

    async def _upload(self, event: ArtifactReady, key: tuple[UUID, UUID]) -> None:
        failure = self._validate(event)
        if failure is not None:
            self._emit_failure(event, failure)
            return
        try:
            with transfer(
                "Upload",
                event.filename,
                event.size_bytes,
                self._progress.writer(event.job_id, JobPhase.UPLOADING),
            ) as report:
                result = await asyncio.wait_for(
                    self._client.upload(
                        event.local_path,
                        caption=event.caption,
                        supports_streaming=_supports_streaming(event),
                        progress_callback=report,
                        file_size=event.size_bytes,
                    ),
                    timeout=self._timeout_seconds,
                )
        except asyncio.CancelledError:
            if not self._shutting_down:
                await self._stop(asyncio.current_task())
            raise
        except TelegramUnavailableError:
            self._emit_failure(event, "telegram_unavailable")
            self._request_unavailable()
            return
        except (asyncio.TimeoutError, TimeoutError):
            self._emit_failure(event, "telegram_timeout")
            return
        except TelegramUploadError:
            self._emit_failure(event, "telegram_upload_failed")
            return
        self._emitted.add(key)
        _LOGGER.info(
            "Artifact uploaded: job=%s artifact=%s chat=%s message=%s",
            event.job_id,
            event.artifact_id,
            result.chat_id,
            result.message_id,
        )
        self._bus.emit(
            ARTIFACT_UPLOADED,
            ArtifactUploaded(
                event.job_id,
                event.artifact_id,
                result.chat_id,
                result.message_id,
                _now(),
            ),
        )

    def _validate(self, event: ArtifactReady) -> str | None:
        try:
            current = event.local_path.lstat()
        except FileNotFoundError:
            return "artifact_missing"
        except OSError:
            return "artifact_invalid"
        if not stat.S_ISREG(current.st_mode):
            return "artifact_invalid"
        try:
            event.local_path.resolve().relative_to(self._storage.root.resolve())
        except (OSError, ValueError):
            return "artifact_outside"
        if event.size_bytes > self._max_bytes:
            return "artifact_oversize"
        try:
            self._storage.validate_staged_artifact(
                StagedArtifact(
                    event.job_id,
                    event.artifact_id,
                    event.local_path,
                    event.filename,
                    event.media_type,
                    event.size_bytes,
                    event.caption,
                )
            )
        except ArtifactStorageError:
            return "artifact_drift"
        return None

    async def _monitor_disconnect(self) -> None:
        try:
            await self._client.wait_until_disconnected()
        except TelegramClientError:
            pass
        if not self._shutting_down:
            self._request_unavailable()

    def _monitor_done(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            self._bus.emit(ERROR, error)

    def _emit_failure(self, event: ArtifactReady, code: str) -> None:
        key = (event.job_id, event.artifact_id)
        if key in self._emitted:
            return
        self._emitted.add(key)
        _LOGGER.warning(
            "Artifact upload failed: job=%s artifact=%s code=%s file=%s bytes=%d",
            event.job_id,
            event.artifact_id,
            code,
            event.filename,
            event.size_bytes,
            # Set when called from an except block, None from a validation
            # rejection; either way the traceback is not lost.
            exc_info=sys.exc_info()[1],
        )
        self._bus.emit(
            ARTIFACT_UPLOAD_FAILED,
            ArtifactUploadFailed(
                event.job_id,
                event.artifact_id,
                ErrorInfo(code, _ERRORS[code]),
                _now(),
            ),
        )

    def _request_unavailable(self) -> None:
        """Hold the pause signal back until in-flight uploads have settled."""
        if self._shutting_down or self._unavailable_emitted:
            return
        if self._inflight:
            self._unavailable_pending = True
            return
        self._emit_unavailable()

    def _flush_unavailable(self) -> None:
        if self._unavailable_pending and not self._inflight:
            self._unavailable_pending = False
            self._emit_unavailable()

    def _emit_unavailable(self) -> None:
        if self._shutting_down or self._unavailable_emitted:
            return
        self._unavailable_emitted = True
        _LOGGER.error(
            "Telegram is unavailable, pausing the queue; restart to resume"
        )
        self._bus.emit(
            TELEGRAM_UNAVAILABLE,
            TelegramUnavailable(
                ErrorInfo("telegram_unavailable", _ERRORS["telegram_unavailable"]),
                _now(),
            ),
        )


def _supports_streaming(event: ArtifactReady) -> bool:
    declared = (event.media_type or "").partition(";")[0].strip().lower()
    if declared.startswith("video/"):
        return True
    guessed, _ = mimetypes.guess_type(event.filename)
    return guessed is not None and guessed.startswith("video/")


def _now() -> datetime:
    return datetime.now(UTC)
