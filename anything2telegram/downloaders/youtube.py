import asyncio
import json
import logging
import mimetypes
import re
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from uuid import UUID, uuid4

from ..artifacts.storage import ArtifactStorage
from ..config import Settings
from ..domain import ErrorInfo, JobPhase, StagedArtifact
from ..events import (
    ARTIFACT_PRODUCTION_FAILED,
    ARTIFACT_READY,
    YOUTUBE_DOWNLOAD_REQUESTED,
    YOUTUBE_PLAYLIST_EXPANDED,
    YOUTUBE_PLAYLIST_EXPANSION_FAILED,
    YOUTUBE_PLAYLIST_EXPANSION_REQUESTED,
    ArtifactProductionFailed,
    ArtifactReady,
    DownloadTarget,
    PlaylistExpanded,
    PlaylistExpansionFailed,
    PlaylistExpansionRequested,
    YouTubeDownloadRequested,
)
from ..jobs.progress import ProgressRegistry
from ..tui import transfer
from .process import ProcessResult, ProcessTimeoutError


_LOGGER = logging.getLogger(__name__)

YTDLP_FORMAT = (
    "bv*[ext=mp4][vcodec^=avc1][height<=1080]+ba[ext=m4a]/"
    "b[ext=mp4][height<=1080]/b[height<=1080]"
)
_VIDEO_ID = re.compile(r"^[A-Za-z0-9_-]{11}$")
_PLAYLIST_ID = re.compile(r"^[A-Za-z0-9_-]+$")
_FINAL_MEDIA_SUFFIXES = frozenset({".mp4", ".mkv", ".webm"})
_UNSAFE_NAME = re.compile(r"[/\x00-\x1f]+")
_YTDLP_COMMAND = (sys.executable, "-m", "yt_dlp")
_QUOTA_POLL_SECONDS = 1.0


class YouTubeUrlKind(str, Enum):
    VIDEO = "video"
    PLAYLIST = "playlist"


class UnsupportedYouTubeUrl(ValueError):
    code = "unsupported_youtube_url"

    def __init__(self) -> None:
        super().__init__("YouTube URL is unsupported")


def classify_youtube_url(url: str) -> YouTubeUrlKind:
    if not isinstance(url, str) or not url.strip():
        raise UnsupportedYouTubeUrl()
    try:
        parsed = urlsplit(url)
        port = parsed.port
    except ValueError:
        raise UnsupportedYouTubeUrl() from None
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or port is not None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise UnsupportedYouTubeUrl()
    host = parsed.hostname.lower().rstrip(".")
    if (
        host != "youtu.be"
        and host != "youtube.com"
        and not host.endswith(".youtube.com")
    ):
        raise UnsupportedYouTubeUrl()

    query = parse_qs(parsed.query, keep_blank_values=True)
    if host == "youtu.be":
        parts = [part for part in parsed.path.split("/") if part]
        if len(parts) == 1 and _valid_video_id(parts[0]):
            return YouTubeUrlKind.VIDEO
        raise UnsupportedYouTubeUrl()

    video_id = None
    if parsed.path == "/watch":
        values = query.get("v", [])
        if len(values) == 1 and _valid_video_id(values[0]):
            video_id = values[0]
    else:
        parts = [part for part in parsed.path.split("/") if part]
        if (
            len(parts) == 2
            and parts[0] in {"shorts", "embed", "v"}
            and _valid_video_id(parts[1])
        ):
            video_id = parts[1]
    if video_id is not None:
        return YouTubeUrlKind.VIDEO

    lists = query.get("list", [])
    if (
        parsed.path in {"", "/", "/playlist"}
        and len(lists) == 1
        and bool(_PLAYLIST_ID.fullmatch(lists[0]))
    ):
        return YouTubeUrlKind.PLAYLIST
    raise UnsupportedYouTubeUrl()


def _valid_video_id(value: str) -> bool:
    return bool(_VIDEO_ID.fullmatch(value))


class YouTubeArtifactProducer:
    def __init__(
        self,
        bus,
        storage: ArtifactStorage,
        process_runner,
        settings: Settings,
        progress_registry: ProgressRegistry,
    ) -> None:
        self._bus = bus
        self._storage = storage
        self._runner = process_runner
        self._timeout_seconds = settings.ytdlp_timeout_seconds
        self._max_artifact_bytes = settings.max_artifact_bytes
        self._cookies_path = settings.cookies_path
        self._cookies_browser = settings.cookies_browser
        self._progress = progress_registry
        bus.on(
            YOUTUBE_PLAYLIST_EXPANSION_REQUESTED,
            self.handle_playlist_expansion_requested,
        )
        bus.on(YOUTUBE_DOWNLOAD_REQUESTED, self.handle_download_requested)

    async def handle_playlist_expansion_requested(
        self, event: PlaylistExpansionRequested
    ) -> None:
        try:
            result = await self._runner.run(
                self._playlist_args(event.source_url, event.offset),
                self._timeout_seconds,
            )
            if result.exit_code != 0:
                self._emit_playlist_failure(
                    event,
                    "youtube_process_failed",
                    "YouTube process failed",
                    _process_detail(result),
                )
                return
            targets, skipped, playlist_title = self._parse_playlist(result.stdout)
            if not targets:
                self._emit_playlist_failure(
                    event,
                    "playlist_empty",
                    "Playlist contains no downloadable videos",
                )
                return
            self._bus.emit(
                YOUTUBE_PLAYLIST_EXPANDED,
                PlaylistExpanded(
                    event.batch_id,
                    tuple(targets),
                    skipped,
                    _now(),
                    playlist_title,
                ),
            )
        except ProcessTimeoutError:
            self._emit_playlist_failure(
                event, "youtube_timeout", "YouTube operation timed out"
            )
        except _PlaylistParseError:
            self._emit_playlist_failure(
                event,
                "playlist_parse_failed",
                "Playlist metadata could not be parsed",
            )
        except Exception:
            self._emit_playlist_failure(
                event, "internal_error", "Playlist expansion failed"
            )

    async def handle_download_requested(
        self, event: YouTubeDownloadRequested
    ) -> None:
        artifact_id: UUID | None = None
        try:
            artifact_id = uuid4()
            directory = self._storage.allocate_download_directory(
                event.job_id, artifact_id
            )
            result = await self._run_download(
                self._download_args(event.source_url, directory),
                event.job_id,
                artifact_id,
                _source_label(event.source_url),
            )
            if result.exit_code != 0:
                self._emit_artifact_failure(
                    event,
                    artifact_id,
                    "youtube_process_failed",
                    "YouTube process failed",
                    _process_detail(result),
                )
                self._storage.delete_job_directory(event.job_id)
                return
            artifact = self._discover_artifact(
                event.job_id, artifact_id, directory, event.caption_prefix
            )
            _LOGGER.info(
                "Artifact produced: job=%s artifact=%s file=%s bytes=%d",
                artifact.job_id,
                artifact.artifact_id,
                artifact.filename,
                artifact.size_bytes,
            )
            self._bus.emit(
                ARTIFACT_READY,
                ArtifactReady(
                    artifact.job_id,
                    artifact.artifact_id,
                    artifact.local_path,
                    artifact.filename,
                    artifact.media_type,
                    artifact.size_bytes,
                    artifact.caption,
                    _now(),
                ),
            )
        except asyncio.CancelledError:
            self._storage.delete_job_directory(event.job_id)
            raise
        except ProcessTimeoutError:
            self._storage.delete_job_directory(event.job_id)
            self._emit_artifact_failure(
                event, artifact_id, "youtube_timeout", "YouTube operation timed out"
            )
        except _ArtifactOversize:
            self._storage.delete_job_directory(event.job_id)
            self._emit_artifact_failure(
                event,
                artifact_id,
                "artifact_oversize",
                "Artifact exceeds maximum allowed size",
            )
        except Exception:
            self._storage.delete_job_directory(event.job_id)
            self._emit_artifact_failure(
                event, artifact_id, "internal_error", "Artifact production failed"
            )
        except BaseException:
            self._storage.delete_job_directory(event.job_id)
            raise

    def _playlist_args(self, source_url: str, offset: int = 0) -> list[str]:
        # --playlist-start is 1-indexed; offset=N skips the first N videos.
        offset_args = ["--playlist-start", str(offset + 1)] if offset > 0 else []
        return [
            *_YTDLP_COMMAND,
            "--flat-playlist",
            "--dump-json",
            "--no-warnings",
            "--ignore-errors",
            *offset_args,
            *self._cookie_args(),
            source_url,
        ]

    def _download_args(self, source_url: str, directory: Path) -> list[str]:
        return [
            *_YTDLP_COMMAND,
            "-f",
            YTDLP_FORMAT,
            "--merge-output-format",
            "mp4",
            "--max-filesize",
            str(self._max_artifact_bytes),
            "--restrict-filenames",
            "--no-playlist",
            "--js-runtimes",
            "node",
            "--remote-components",
            "ejs:github",
            "-o",
            str(directory / "%(title).100s.%(ext)s"),
            *self._cookie_args(),
            source_url,
        ]

    async def _run_download(
        self, args: Sequence[str], job_id: UUID, artifact_id: UUID, label: str
    ) -> ProcessResult:
        """Waits for yt-dlp, stopping it if the partial download outgrows its quota."""
        runner_task = asyncio.create_task(
            self._runner.run(args, self._timeout_seconds)
        )
        try:
            # ponytail: bytes on disk, so no percentage — the final size is
            # unknown until yt-dlp merges. Parse --progress-template if a
            # percentage is ever worth streaming yt-dlp's stdout for.
            with transfer(
                "Download", label, 0, self._progress.writer(job_id, JobPhase.PRODUCING)
            ) as report:
                while True:
                    done, _ = await asyncio.wait(
                        (runner_task,), timeout=_QUOTA_POLL_SECONDS
                    )
                    size = self._storage.download_directory_size(job_id, artifact_id)
                    report(size, 0)
                    if size > self._max_artifact_bytes:
                        raise _ArtifactOversize()
                    if done:
                        return await runner_task
        finally:
            if not runner_task.done():
                runner_task.cancel()
                await asyncio.gather(runner_task, return_exceptions=True)

    def _cookie_args(self) -> list[str]:
        # Never both: given a cookie file, yt-dlp writes the merged jar back to
        # it on exit, which would spill every cookie in the browser onto disk.
        if self._cookies_browser is not None:
            return ["--cookies-from-browser", self._cookies_browser]
        if self._cookies_path is None:
            return []
        return ["--cookies", str(self._cookies_path)]

    @staticmethod
    def _parse_playlist(stdout: str) -> tuple[list[DownloadTarget], int, str | None]:
        targets: list[DownloadTarget] = []
        seen: set[str] = set()
        skipped = 0
        playlist_title: str | None = None
        for line in (line for line in stdout.splitlines() if line.strip()):
            try:
                entry = json.loads(line)
            except (json.JSONDecodeError, TypeError):
                raise _PlaylistParseError() from None
            if not isinstance(entry, dict):
                skipped += 1
                continue
            source_id = entry.get("id")
            # Availability is not filtered here: cookies auth downloads member-only
            # videos, and unavailable entries fail per-job downstream (visible, not
            # silently dropped). ponytail: re-add a {"private","unavailable"} skip
            # if dead-video failure spam becomes a problem.
            if (
                not isinstance(source_id, str)
                or not _valid_video_id(source_id)
                or source_id in seen
            ):
                skipped += 1
                continue
            seen.add(source_id)
            if playlist_title is None:
                playlist_title = _playlist_title(entry)
            title = entry.get("title")
            targets.append(
                DownloadTarget(
                    source_id,
                    f"https://www.youtube.com/watch?v={source_id}",
                    _caption_prefix(entry),
                    title=title if isinstance(title, str) else None,
                )
            )
        return targets, skipped, playlist_title

    def _discover_artifact(
        self,
        job_id: UUID,
        artifact_id: UUID,
        directory: Path,
        caption_prefix: str = "",
    ) -> StagedArtifact:
        candidates = [
            entry
            for entry in directory.iterdir()
            if entry.suffix.lower() in _FINAL_MEDIA_SUFFIXES
            and not entry.is_symlink()
            and entry.is_file()
        ]
        if len(candidates) != 1:
            raise RuntimeError("download output is missing or ambiguous")
        path = candidates[0]
        caption = caption_prefix + path.stem.replace("_", " ")
        path = path.rename(path.with_name(_caption_filename(caption, path.suffix)))
        return StagedArtifact(
            job_id,
            artifact_id,
            path,
            path.name,
            mimetypes.guess_type(path.name)[0],
            path.stat().st_size,
            caption,
        )

    def _emit_playlist_failure(
        self,
        event: PlaylistExpansionRequested,
        code: str,
        message: str,
        detail: str = "",
    ) -> None:
        _LOGGER.warning(
            "Playlist expansion failed: batch=%s code=%s url=%s%s",
            event.batch_id,
            code,
            event.source_url,
            f" {detail}" if detail else "",
            exc_info=sys.exc_info()[1],
        )
        self._bus.emit(
            YOUTUBE_PLAYLIST_EXPANSION_FAILED,
            PlaylistExpansionFailed(
                event.batch_id,
                ErrorInfo(code, message),
                _now(),
            ),
        )

    def _emit_artifact_failure(
        self,
        event: YouTubeDownloadRequested,
        artifact_id: UUID | None,
        code: str,
        message: str,
        detail: str = "",
    ) -> None:
        _LOGGER.warning(
            "Artifact production failed: job=%s artifact=%s code=%s url=%s%s",
            event.job_id,
            artifact_id,
            code,
            event.source_url,
            f" {detail}" if detail else "",
            exc_info=sys.exc_info()[1],
        )
        self._bus.emit(
            ARTIFACT_PRODUCTION_FAILED,
            ArtifactProductionFailed(
                event.job_id,
                artifact_id,
                ErrorInfo(code, message),
                _now(),
            ),
        )

def _process_detail(result: ProcessResult) -> str:
    """yt-dlp's own explanation, for the operator log only."""
    return f"exit={result.exit_code} stderr={result.stderr_tail!r}"


def _source_label(source_url: str) -> str:
    """The video id, which is all a progress bar has before the file exists."""
    return source_url.rsplit("/", 1)[-1].rsplit("=", 1)[-1]


def _playlist_title(entry: dict) -> str | None:
    title = entry.get("playlist_title") or entry.get("playlist")
    return title.strip() if isinstance(title, str) and title.strip() else None


def _caption_prefix(entry: dict) -> str:
    """'Playlist name - 03/42 - ' from flat-playlist metadata, '' when absent."""
    parts = []
    title = _playlist_title(entry)
    if title is not None:
        parts.append(title)
    index = entry.get("playlist_index")
    total = entry.get("playlist_count")
    if isinstance(index, int) and not isinstance(index, bool) and index > 0:
        if isinstance(total, int) and not isinstance(total, bool) and total >= index:
            parts.append(f"{index:0{len(str(total))}d}/{total}")
        else:
            parts.append(str(index))
    return " - ".join(parts) + " - " if parts else ""


def _caption_filename(caption: str, suffix: str) -> str:
    """The caption as a filename, so Telegram shows the caption as the name too."""
    # ponytail: only the bytes a POSIX path cannot hold are replaced; keep the
    # spaces, they are the point. The 255-byte cap is the filesystem's limit.
    stem = _UNSAFE_NAME.sub("_", caption).strip(" .")
    limit = 255 - len(suffix.encode())
    stem = stem.encode()[:limit].decode(errors="ignore").rstrip(" .")
    return (stem or "video") + suffix


class _PlaylistParseError(ValueError):
    pass


class _ArtifactOversize(Exception):
    pass


def _now() -> datetime:
    return datetime.now(UTC)
