import asyncio
import json
from collections.abc import Sequence
from pathlib import Path

import pytest

from anything2telegram import main
from anything2telegram.config import Settings
from anything2telegram.domain import ProcessResult, TelegramUploadResult


class FakeYouTubeRunner:
    """Stands in for yt-dlp: records calls and writes the output file itself."""

    def __init__(
        self,
        *,
        playlist_entries: list[dict[str, object]] | None = None,
        blocked_playlist: bool = False,
        failed_urls: set[str] | None = None,
    ) -> None:
        self.playlist_entries = playlist_entries or []
        self.failed_urls = failed_urls or set()
        self.calls: list[tuple[str, str]] = []
        self.playlist_started = asyncio.Event()
        self.release_playlist = asyncio.Event()
        if not blocked_playlist:
            self.release_playlist.set()

    async def run(
        self, args: Sequence[str], _timeout_seconds: float
    ) -> ProcessResult:
        source_url = args[-1]
        if "--flat-playlist" in args:
            self.calls.append(("playlist", source_url))
            self.playlist_started.set()
            await self.release_playlist.wait()
            stdout = "\n".join(json.dumps(entry) for entry in self.playlist_entries)
            return ProcessResult(0, stdout, "")

        self.calls.append(("download", source_url))
        if source_url in self.failed_urls:
            return ProcessResult(1, "", "process failed")
        template = Path(args[args.index("-o") + 1])
        template.parent.joinpath(f"{source_url[-11:]}.mp4").write_bytes(
            source_url.encode()
        )
        return ProcessResult(0, "", "")


class FakeTelegram:
    """Stands in for TelegramClientAdapter, one release event per upload."""

    def __init__(self, *, manual_release: bool = False) -> None:
        self.connected = False
        self.connect_error: BaseException | None = None
        self.disconnected = asyncio.Event()
        self.started = [asyncio.Event() for _ in range(8)]
        self.releases = [asyncio.Event() for _ in range(8)]
        if not manual_release:
            for release in self.releases:
                release.set()
        self.upload_errors: list[BaseException | None] = []
        self.uploaded_filenames: list[str] = []
        self.captions: list[str | None] = []
        self.messages: list[str] = []
        self.active_uploads = 0
        self.max_active_uploads = 0

    @property
    def is_connected(self) -> bool:
        return self.connected

    async def connect(self) -> None:
        if self.connect_error is not None:
            raise self.connect_error
        self.connected = True

    async def disconnect(self) -> None:
        self.connected = False
        self.disconnected.set()

    async def wait_until_disconnected(self) -> None:
        await self.disconnected.wait()

    def drop(self) -> None:
        """Simulates Telegram going away underneath a live client."""
        self.connected = False
        self.disconnected.set()

    async def send_message(self, text: str) -> None:
        self.messages.append(text)

    async def upload(
        self, path: Path, *, caption: str | None = None, **_kwargs: object
    ) -> TelegramUploadResult:
        index = len(self.uploaded_filenames)
        self.uploaded_filenames.append(path.name)
        self.captions.append(caption)
        self.active_uploads += 1
        self.max_active_uploads = max(self.max_active_uploads, self.active_uploads)
        self.started[index].set()
        try:
            await self.releases[index].wait()
            if index < len(self.upload_errors) and self.upload_errors[index]:
                raise self.upload_errors[index]
            return TelegramUploadResult(-1001, index + 1)
        finally:
            self.active_uploads -= 1


def settings_for(tmp_path: Path, **overrides: object) -> Settings:
    values: dict[str, object] = {
        "api_id": 1,
        "api_hash": "unused",
        "bot_token": "unused",
        "channel_id": -1001,
        "session_path": tmp_path / "unused.session",
        "cookies_path": None,
        "artifact_root": tmp_path / "artifacts",
        "max_artifact_bytes": 4096,
        "ytdlp_timeout_seconds": 2,
        "tg_upload_timeout_seconds": 2,
        "shutdown_grace_seconds": 1,
        "log_level": "DEBUG",
    }
    return Settings(**(values | overrides))


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return settings_for(tmp_path)


@pytest.fixture
def build_app(monkeypatch: pytest.MonkeyPatch, settings: Settings):
    """Builds the real app with yt-dlp and Telegram swapped for fakes."""

    def build(
        runner: FakeYouTubeRunner | None = None,
        telegram: FakeTelegram | None = None,
        app_settings: Settings | None = None,
    ):
        resolved_runner = runner if runner is not None else FakeYouTubeRunner()
        resolved_telegram = telegram if telegram is not None else FakeTelegram()
        monkeypatch.setattr(main, "YouTubeProcessRunner", lambda: resolved_runner)
        monkeypatch.setattr(
            main, "TelegramClientAdapter", lambda _settings: resolved_telegram
        )
        return main.create_app(app_settings if app_settings else settings)

    return build
