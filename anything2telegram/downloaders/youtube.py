import asyncio
import json
import mimetypes
import re
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Protocol
from urllib.parse import parse_qs, urlsplit
from uuid import UUID, uuid4

from ..artifacts.storage import ArtifactStorage
from ..bus import EventBus
from ..domain import ErrorInfo, StagedArtifact
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
from .process import ProcessResult, ProcessTimeoutError


YTDLP_FORMAT = (
    "bv*[ext=mp4][vcodec^=avc1][height<=1080]+ba[ext=m4a]/"
    "b[ext=mp4][height<=1080]/b[height<=1080]"
)
_VIDEO_ID = re.compile(r"^[A-Za-z0-9_-]{11}$")
_PLAYLIST_ID = re.compile(r"^[A-Za-z0-9_-]+$")
_FINAL_MEDIA_SUFFIXES = frozenset({".mp4", ".mkv", ".webm"})
_UNAVAILABLE = frozenset(
    {"private", "premium_only", "subscriber_only", "needs_auth", "unavailable"}
)


class YouTubeUrlKind(str, Enum):
    VIDEO = "video"
    PLAYLIST = "playlist"


class UnsupportedYouTubeUrl(ValueError):
    code = "unsupported_youtube_url"

    def __init__(self) -> None:
        super().__init__("YouTube URL is unsupported")


class _ProcessRunner(Protocol):
    async def run(
        self, args: Sequence[str], timeout_seconds: float | int
    ) -> ProcessResult: ...


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
        bus: EventBus,
        storage: ArtifactStorage,
        process_runner: _ProcessRunner,
        *,
        timeout_seconds: float | int,
        cookies_path: Path | None = None,
        format_selector: str = YTDLP_FORMAT,
        id_factory: Callable[[], UUID] = uuid4,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if type(timeout_seconds) not in (int, float) or timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if cookies_path is not None and not isinstance(cookies_path, Path):
            raise TypeError("cookies_path must be Path or None")
        if not isinstance(format_selector, str) or not format_selector:
            raise ValueError("format_selector must be nonblank")
        self._bus = bus
        self._storage = storage
        self._runner = process_runner
        self._timeout_seconds = timeout_seconds
        self._cookies_path = cookies_path
        self._format_selector = format_selector
        self._id_factory = id_factory
        self._clock = clock
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
                self._playlist_args(event.source_url), self._timeout_seconds
            )
            if result.exit_code != 0:
                self._emit_playlist_failure(
                    event, "youtube_process_failed", "YouTube process failed"
                )
                return
            targets, skipped = self._parse_playlist(result.stdout)
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
                    self._causal_time(event.occurred_at),
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
        artifact_id = self._id_factory()
        try:
            directory = self._storage.allocate_download_directory(
                event.job_id, artifact_id
            )
            result = await self._runner.run(
                self._download_args(event.source_url, directory),
                self._timeout_seconds,
            )
            if result.exit_code != 0:
                self._emit_artifact_failure(
                    event,
                    artifact_id,
                    "youtube_process_failed",
                    "YouTube process failed",
                )
                self._best_effort_cleanup(event.job_id)
                return
            artifact = self._discover_artifact(event.job_id, artifact_id, directory)
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
                    self._causal_time(event.occurred_at),
                ),
            )
        except asyncio.CancelledError:
            self._best_effort_cleanup(event.job_id)
            raise
        except ProcessTimeoutError:
            self._best_effort_cleanup(event.job_id)
            self._emit_artifact_failure(
                event, artifact_id, "youtube_timeout", "YouTube operation timed out"
            )
        except Exception:
            self._best_effort_cleanup(event.job_id)
            self._emit_artifact_failure(
                event, artifact_id, "internal_error", "Artifact production failed"
            )
        except BaseException:
            self._best_effort_cleanup(event.job_id)
            raise

    def _playlist_args(self, source_url: str) -> list[str]:
        return [
            "yt-dlp",
            "--flat-playlist",
            "--dump-json",
            "--no-warnings",
            "--ignore-errors",
            *self._cookie_args(),
            source_url,
        ]

    def _download_args(self, source_url: str, directory: Path) -> list[str]:
        return [
            "yt-dlp",
            "-f",
            self._format_selector,
            "--merge-output-format",
            "mp4",
            "--restrict-filenames",
            "--no-playlist",
            "--js-runtimes",
            "node",
            "--remote-components",
            "ejs:github",
            "-o",
            str(directory / "%(title).80s.%(ext)s"),
            *self._cookie_args(),
            source_url,
        ]

    def _cookie_args(self) -> list[str]:
        if self._cookies_path is None:
            return []
        return ["--cookies", str(self._cookies_path)]

    @staticmethod
    def _parse_playlist(stdout: str) -> tuple[list[DownloadTarget], int]:
        targets: list[DownloadTarget] = []
        seen: set[str] = set()
        skipped = 0
        nonblank_lines = [line for line in stdout.splitlines() if line.strip()]
        if not nonblank_lines:
            return targets, 0
        for line in nonblank_lines:
            try:
                entry = json.loads(line)
            except (json.JSONDecodeError, TypeError):
                raise _PlaylistParseError() from None
            if not isinstance(entry, dict):
                skipped += 1
                continue
            source_id = entry.get("id")
            availability = entry.get("availability")
            if (
                not isinstance(source_id, str)
                or not _valid_video_id(source_id)
                or availability in _UNAVAILABLE
                or source_id in seen
            ):
                skipped += 1
                continue
            seen.add(source_id)
            targets.append(
                DownloadTarget(
                    source_id,
                    f"https://www.youtube.com/watch?v={source_id}",
                )
            )
        return targets, skipped

    def _discover_artifact(
        self, job_id: UUID, artifact_id: UUID, directory: Path
    ) -> StagedArtifact:
        resolved_directory = directory.resolve(strict=True)
        candidates: list[Path] = []
        for entry in directory.iterdir():
            if (
                entry.suffix.lower() not in _FINAL_MEDIA_SUFFIXES
                or entry.is_symlink()
                or not entry.is_file()
            ):
                continue
            resolved = entry.resolve(strict=True)
            try:
                resolved.relative_to(resolved_directory)
            except ValueError:
                continue
            candidates.append(entry)
        if len(candidates) != 1:
            raise RuntimeError("download output is missing or ambiguous")
        path = candidates[0]
        size = path.stat().st_size
        media_type = mimetypes.guess_type(path.name)[0]
        staged = StagedArtifact(
            job_id,
            artifact_id,
            path,
            path.name,
            media_type,
            size,
            path.stem.replace("_", " "),
        )
        self._storage.validate_staged_artifact(staged)
        return staged

    def _emit_playlist_failure(
        self, event: PlaylistExpansionRequested, code: str, message: str
    ) -> None:
        self._bus.emit(
            YOUTUBE_PLAYLIST_EXPANSION_FAILED,
            PlaylistExpansionFailed(
                event.batch_id,
                ErrorInfo(code, message),
                self._causal_time(event.occurred_at),
            ),
        )

    def _emit_artifact_failure(
        self,
        event: YouTubeDownloadRequested,
        artifact_id: UUID,
        code: str,
        message: str,
    ) -> None:
        self._bus.emit(
            ARTIFACT_PRODUCTION_FAILED,
            ArtifactProductionFailed(
                event.job_id,
                artifact_id,
                ErrorInfo(code, message),
                self._causal_time(event.occurred_at),
            ),
        )

    def _causal_time(self, occurred_at: datetime) -> datetime:
        return max(self._clock(), occurred_at)

    def _best_effort_cleanup(self, job_id: UUID) -> None:
        try:
            self._storage.delete_job_directory(job_id)
        except Exception:
            pass


class _PlaylistParseError(ValueError):
    pass
