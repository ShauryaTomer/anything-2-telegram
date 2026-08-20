import asyncio
import json
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from pyee.asyncio import AsyncIOEventEmitter

from anything2telegram.artifacts.storage import ArtifactStorage
from anything2telegram.config import Settings
from anything2telegram.domain import ProcessResult
from anything2telegram.downloaders.process import ProcessTimeoutError
from anything2telegram.downloaders.youtube import (
    ChannelListingError,
    ChannelPlaylist,
    UnsupportedYouTubeUrl,
    YouTubeArtifactProducer,
    YouTubeUrlKind,
    _caption_filename,
    channel_playlists_url,
    classify_youtube_url,
)
from anything2telegram.events import (
    ARTIFACT_PRODUCTION_FAILED,
    ARTIFACT_READY,
    YOUTUBE_PLAYLIST_EXPANDED,
    YOUTUBE_PLAYLIST_EXPANSION_FAILED,
    PlaylistExpansionRequested,
    YouTubeDownloadRequested,
)
from anything2telegram.jobs.progress import ProgressRegistry
from tests.conftest import settings_for


VIDEO_ID = "aaaaaaaaaaa"


@pytest.mark.parametrize(
    "url,kind",
    [
        ("https://youtu.be/aaaaaaaaaaa", YouTubeUrlKind.VIDEO),
        ("https://www.youtube.com/watch?v=aaaaaaaaaaa", YouTubeUrlKind.VIDEO),
        ("https://youtube.com/shorts/aaaaaaaaaaa", YouTubeUrlKind.VIDEO),
        ("https://m.youtube.com/embed/aaaaaaaaaaa", YouTubeUrlKind.VIDEO),
        ("https://www.youtube.com/v/aaaaaaaaaaa", YouTubeUrlKind.VIDEO),
        # A video id wins even when the URL also carries a playlist.
        (
            "https://www.youtube.com/watch?v=aaaaaaaaaaa&list=PL123",
            YouTubeUrlKind.VIDEO,
        ),
        ("https://www.youtube.com/playlist?list=PL123", YouTubeUrlKind.PLAYLIST),
        ("https://www.youtube.com/?list=PL123", YouTubeUrlKind.PLAYLIST),
        ("https://www.youtube.com/@Java.Brains", YouTubeUrlKind.CHANNEL),
        ("https://www.youtube.com/@Java.Brains/playlists", YouTubeUrlKind.CHANNEL),
        ("https://www.youtube.com/channel/UC123abc", YouTubeUrlKind.CHANNEL),
        ("https://www.youtube.com/c/JavaBrains", YouTubeUrlKind.CHANNEL),
        ("https://www.youtube.com/user/koushks", YouTubeUrlKind.CHANNEL),
    ],
)
def test_supported_urls_are_classified(url: str, kind: YouTubeUrlKind) -> None:
    assert classify_youtube_url(url) is kind


@pytest.mark.parametrize(
    "url",
    [
        "",
        "   ",
        "not-a-url",
        "ftp://youtube.com/watch?v=aaaaaaaaaaa",
        "https://evil.com/watch?v=aaaaaaaaaaa",
        "https://youtube.com.evil.com/watch?v=aaaaaaaaaaa",
        "https://user:pass@youtube.com/watch?v=aaaaaaaaaaa",
        "https://youtube.com:8080/watch?v=aaaaaaaaaaa",
        "https://www.youtube.com/watch?v=tooshort",
        "https://www.youtube.com/watch?v=aaaaaaaaaaa&v=bbbbbbbbbbb",
        "https://youtu.be/aaaaaaaaaaa/extra",
        "https://www.youtube.com/watch?v=aaaaaaaaaaa#fragment",
        "https://www.youtube.com/feed/subscriptions",
    ],
)
def test_unsupported_urls_are_rejected(url: str) -> None:
    with pytest.raises(UnsupportedYouTubeUrl):
        classify_youtube_url(url)


class ScriptedRunner:
    """Returns a queued result per call and records the argument vectors."""

    def __init__(self, *results: object) -> None:
        self.results = list(results)
        self.calls: list[Sequence[str]] = []

    async def run(self, args: Sequence[str], timeout_seconds: float) -> ProcessResult:
        self.calls.append(list(args))
        self.timeout_seconds = timeout_seconds
        result = self.results.pop(0)
        if isinstance(result, BaseException):
            raise result
        if callable(result):
            return result(args)
        return result


class Recorder:
    def __init__(self, bus: AsyncIOEventEmitter) -> None:
        self.expanded: list[object] = []
        self.expansion_failed: list[object] = []
        self.ready: list[object] = []
        self.production_failed: list[object] = []
        bus.on(YOUTUBE_PLAYLIST_EXPANDED, self.expanded.append)
        bus.on(YOUTUBE_PLAYLIST_EXPANSION_FAILED, self.expansion_failed.append)
        bus.on(ARTIFACT_READY, self.ready.append)
        bus.on(ARTIFACT_PRODUCTION_FAILED, self.production_failed.append)

    def failure_codes(self) -> list[str]:
        return [event.error.code for event in self.production_failed]

    def expansion_failure_codes(self) -> list[str]:
        return [event.error.code for event in self.expansion_failed]


@pytest.fixture
def bus() -> AsyncIOEventEmitter:
    return AsyncIOEventEmitter()


@pytest.fixture
def recorder(bus: AsyncIOEventEmitter) -> Recorder:
    return Recorder(bus)


@pytest.fixture
def storage(tmp_path: Path) -> ArtifactStorage:
    return ArtifactStorage(tmp_path / "artifacts")


def make_producer(bus, storage, runner, settings: Settings):
    return YouTubeArtifactProducer(bus, storage, runner, settings, ProgressRegistry())


async def settle(bus: AsyncIOEventEmitter) -> None:
    while not bus.complete:
        await asyncio.wait_for(bus.wait_for_complete(), timeout=2)


def playlist_stdout(*entries: dict[str, object]) -> str:
    return "\n".join(json.dumps(entry) for entry in entries)


def writes_file(name: str, payload: bytes = b"payload"):
    def run(args: Sequence[str]) -> ProcessResult:
        template = Path(args[args.index("-o") + 1])
        template.parent.joinpath(name).write_bytes(payload)
        return ProcessResult(0, "", "")

    return run


def expansion_request(offset: int = 0) -> PlaylistExpansionRequested:
    return PlaylistExpansionRequested(
        uuid4(), "https://www.youtube.com/playlist?list=PL123", datetime.now(UTC), offset
    )


def download_request() -> YouTubeDownloadRequested:
    return YouTubeDownloadRequested(
        uuid4(), f"https://www.youtube.com/watch?v={VIDEO_ID}", datetime.now(UTC)
    )


async def test_a_playlist_is_expanded_into_canonical_watch_urls(
    bus, storage, settings, recorder
) -> None:
    runner = ScriptedRunner(
        ProcessResult(
            0,
            playlist_stdout({"id": VIDEO_ID}, {"id": "bbbbbbbbbbb"}),
            "",
        )
    )
    make_producer(bus, storage, runner, settings)

    bus.emit("youtube.playlist.expansion.requested", expansion_request())
    await settle(bus)

    assert len(recorder.expanded) == 1
    targets = recorder.expanded[0].targets
    assert [target.source_url for target in targets] == [
        f"https://www.youtube.com/watch?v={VIDEO_ID}",
        "https://www.youtube.com/watch?v=bbbbbbbbbbb",
    ]
    assert recorder.expanded[0].skipped_entries == 0


async def test_duplicate_and_unusable_playlist_entries_are_counted_as_skipped(
    bus, storage, settings, recorder
) -> None:
    runner = ScriptedRunner(
        ProcessResult(
            0,
            playlist_stdout(
                {"id": VIDEO_ID},
                {"id": VIDEO_ID},
                {"id": "too-short"},
                {"no_id": True},
            ),
            "",
        )
    )
    make_producer(bus, storage, runner, settings)

    bus.emit("youtube.playlist.expansion.requested", expansion_request())
    await settle(bus)

    assert len(recorder.expanded[0].targets) == 1
    assert recorder.expanded[0].skipped_entries == 3


async def test_playlist_entries_carry_a_numbered_caption_prefix(
    bus, storage, settings, recorder
) -> None:
    entry = {
        "playlist_title": "Rust Fundamentals ",
        "playlist_count": 12,
        "playlist_index": 3,
    }
    runner = ScriptedRunner(
        ProcessResult(
            0,
            playlist_stdout(
                {"id": VIDEO_ID, **entry},
                {"id": "bbbbbbbbbbb", **entry, "playlist_index": 11},
                {"id": "ccccccccccc"},
            ),
            "",
        )
    )
    make_producer(bus, storage, runner, settings)

    bus.emit("youtube.playlist.expansion.requested", expansion_request())
    await settle(bus)

    expanded = recorder.expanded[0]
    assert expanded.playlist_title == "Rust Fundamentals"
    assert [target.caption_prefix for target in expanded.targets] == [
        "Rust Fundamentals - 03/12 - ",
        "Rust Fundamentals - 11/12 - ",
        "",
    ]


async def test_playlist_entries_carry_their_own_title(
    bus, storage, settings, recorder
) -> None:
    runner = ScriptedRunner(
        ProcessResult(
            0,
            playlist_stdout(
                {"id": VIDEO_ID, "title": "Episode One"},
                {"id": "bbbbbbbbbbb"},
            ),
            "",
        )
    )
    make_producer(bus, storage, runner, settings)

    bus.emit("youtube.playlist.expansion.requested", expansion_request())
    await settle(bus)

    assert [target.title for target in recorder.expanded[0].targets] == [
        "Episode One",
        None,
    ]


async def test_a_caption_prefix_is_prepended_to_the_artifact_caption(
    bus, storage, settings, recorder
) -> None:
    runner = ScriptedRunner(writes_file("My_Great_Clip.mp4"))
    make_producer(bus, storage, runner, settings)
    request = download_request()
    event = YouTubeDownloadRequested(
        request.job_id, request.source_url, request.occurred_at, "Rust - 03/12 - "
    )

    bus.emit("youtube.download.requested", event)
    await settle(bus)

    assert recorder.ready[0].caption == "Rust - 03/12 - My Great Clip"
    # The slash cannot survive in a path, everything else can.
    assert recorder.ready[0].filename == "Rust - 03_12 - My Great Clip.mp4"
    assert recorder.ready[0].local_path.name == recorder.ready[0].filename


async def test_an_offset_becomes_a_one_indexed_playlist_start(
    bus, storage, settings, recorder
) -> None:
    runner = ScriptedRunner(
        ProcessResult(0, playlist_stdout({"id": VIDEO_ID}), ""),
        ProcessResult(0, playlist_stdout({"id": VIDEO_ID}), ""),
    )
    make_producer(bus, storage, runner, settings)

    bus.emit("youtube.playlist.expansion.requested", expansion_request(offset=3))
    await settle(bus)
    assert "--playlist-start" in runner.calls[0]
    assert runner.calls[0][runner.calls[0].index("--playlist-start") + 1] == "4"

    bus.emit("youtube.playlist.expansion.requested", expansion_request(offset=0))
    await settle(bus)
    assert "--playlist-start" not in runner.calls[1]


@pytest.mark.parametrize(
    "result,code",
    [
        (ProcessResult(1, "", "boom"), "youtube_process_failed"),
        (ProcessResult(0, "", ""), "playlist_empty"),
        (ProcessResult(0, "not json", ""), "playlist_parse_failed"),
        (ProcessTimeoutError(), "youtube_timeout"),
        (RuntimeError("boom"), "internal_error"),
    ],
)
async def test_expansion_failures_map_to_stable_codes(
    bus, storage, settings, recorder, result: object, code: str
) -> None:
    make_producer(bus, storage, ScriptedRunner(result), settings)

    bus.emit("youtube.playlist.expansion.requested", expansion_request())
    await settle(bus)

    assert recorder.expansion_failure_codes() == [code]
    assert recorder.expanded == []


async def test_a_playlist_of_only_unusable_entries_is_reported_empty(
    bus, storage, settings, recorder
) -> None:
    runner = ScriptedRunner(
        ProcessResult(0, playlist_stdout({"id": "too-short"}), "")
    )
    make_producer(bus, storage, runner, settings)

    bus.emit("youtube.playlist.expansion.requested", expansion_request())
    await settle(bus)

    assert recorder.expansion_failure_codes() == ["playlist_empty"]


async def test_a_download_produces_a_ready_artifact_with_a_readable_caption(
    bus, storage, settings, recorder
) -> None:
    runner = ScriptedRunner(writes_file("My_Great_Clip.mp4"))
    make_producer(bus, storage, runner, settings)
    event = download_request()

    bus.emit("youtube.download.requested", event)
    await settle(bus)

    assert len(recorder.ready) == 1
    ready = recorder.ready[0]
    assert ready.job_id == event.job_id
    assert ready.filename == "My Great Clip.mp4"
    assert ready.local_path.name == "My Great Clip.mp4"
    assert ready.caption == "My Great Clip"
    assert ready.media_type == "video/mp4"
    assert ready.size_bytes == 7
    assert ready.local_path.parent.parent == storage.root / str(event.job_id)


async def test_a_download_reports_its_progress(
    bus, storage, settings, recorder, caplog
) -> None:
    runner = ScriptedRunner(writes_file("clip.mp4", b"x" * 1500))
    make_producer(bus, storage, runner, settings)

    with caplog.at_level("INFO", logger="anything2telegram"):
        bus.emit("youtube.download.requested", download_request())
        await settle(bus)

    assert "Download progress: 0.0 MB" in caplog.text
    assert len(recorder.ready) == 1


async def test_the_download_command_bounds_size_and_uses_the_running_interpreter(
    bus, storage, settings, recorder
) -> None:
    runner = ScriptedRunner(writes_file("clip.mp4"))
    make_producer(bus, storage, runner, settings)

    bus.emit("youtube.download.requested", download_request())
    await settle(bus)

    args = runner.calls[0]
    assert args[:3] == [sys.executable, "-m", "yt_dlp"]
    video_budget = settings.max_artifact_bytes * 4 // 5
    audio_budget = settings.max_artifact_bytes - video_budget
    selected_format = args[args.index("-f") + 1]
    assert f"[filesize<={video_budget}]" in selected_format
    assert f"[filesize<={audio_budget}]" in selected_format
    assert args[args.index("--max-filesize") + 1] == str(settings.max_artifact_bytes)
    assert "--no-playlist" in args
    assert runner.timeout_seconds == settings.ytdlp_timeout_seconds


async def test_cookies_are_passed_only_when_configured(
    bus, storage, recorder, tmp_path
) -> None:
    cookies = tmp_path / "cookies.txt"
    cookies.write_text("# netscape")
    with_cookies = settings_for(tmp_path, cookies_path=cookies)
    runner = ScriptedRunner(writes_file("clip.mp4"))
    make_producer(bus, storage, runner, with_cookies)

    bus.emit("youtube.download.requested", download_request())
    await settle(bus)

    args = runner.calls[0]
    assert args[args.index("--cookies") + 1] == str(cookies)


async def test_a_browser_profile_replaces_the_cookie_file(
    bus, storage, recorder, tmp_path
) -> None:
    cookies = tmp_path / "cookies.txt"
    cookies.write_text("# netscape")
    from_browser = settings_for(
        tmp_path, cookies_path=cookies, cookies_browser="brave:Profile 3"
    )
    runner = ScriptedRunner(writes_file("clip.mp4"))
    make_producer(bus, storage, runner, from_browser)

    bus.emit("youtube.download.requested", download_request())
    await settle(bus)

    args = runner.calls[0]
    assert args[args.index("--cookies-from-browser") + 1] == "brave:Profile 3"
    # Both flags together would make yt-dlp write the browser's cookies to disk.
    assert "--cookies" not in args


@pytest.mark.parametrize(
    "result,code",
    [
        (ProcessResult(1, "", "boom"), "youtube_process_failed"),
        (ProcessTimeoutError(), "youtube_timeout"),
        (RuntimeError("boom"), "internal_error"),
        # yt-dlp exited cleanly but left nothing usable behind.
        (ProcessResult(0, "", ""), "internal_error"),
    ],
)
async def test_download_failures_map_to_codes_and_clean_up(
    bus, storage, settings, recorder, result: object, code: str
) -> None:
    make_producer(bus, storage, ScriptedRunner(result), settings)
    event = download_request()

    bus.emit("youtube.download.requested", event)
    await settle(bus)

    assert recorder.failure_codes() == [code]
    assert recorder.ready == []
    assert not (storage.root / str(event.job_id)).exists()


async def test_an_ambiguous_download_output_is_refused(
    bus, storage, settings, recorder
) -> None:
    def two_files(args: Sequence[str]) -> ProcessResult:
        directory = Path(args[args.index("-o") + 1]).parent
        directory.joinpath("one.mp4").write_bytes(b"a")
        directory.joinpath("two.mkv").write_bytes(b"b")
        return ProcessResult(0, "", "")

    make_producer(bus, storage, ScriptedRunner(two_files), settings)

    bus.emit("youtube.download.requested", download_request())
    await settle(bus)

    assert recorder.failure_codes() == ["internal_error"]


async def test_a_download_that_outgrows_the_limit_is_stopped(
    bus, storage, recorder, tmp_path
) -> None:
    small = settings_for(tmp_path, max_artifact_bytes=8)
    make_producer(bus, storage, ScriptedRunner(writes_file("clip.mp4", b"x" * 64)), small)
    event = download_request()

    bus.emit("youtube.download.requested", event)
    await settle(bus)

    assert recorder.failure_codes() == ["artifact_oversize"]
    assert not (storage.root / str(event.job_id)).exists()


async def test_temporary_merge_files_do_not_count_as_an_oversize_artifact(
    bus, storage, recorder, tmp_path
) -> None:
    def merged_download(args: Sequence[str]) -> ProcessResult:
        directory = Path(args[args.index("-o") + 1]).parent
        directory.joinpath("clip.f140.m4a").write_bytes(b"x" * 6)
        directory.joinpath("clip.mp4").write_bytes(b"x" * 7)
        return ProcessResult(0, "", "")

    small = settings_for(tmp_path, max_artifact_bytes=8)
    make_producer(bus, storage, ScriptedRunner(merged_download), small)

    bus.emit("youtube.download.requested", download_request())
    await settle(bus)

    assert recorder.failure_codes() == []
    assert recorder.ready[0].size_bytes == 7


async def test_cancellation_cleans_up_and_propagates(
    bus, storage, settings, recorder
) -> None:
    producer = make_producer(
        bus, storage, ScriptedRunner(asyncio.CancelledError()), settings
    )
    event = download_request()

    with pytest.raises(asyncio.CancelledError):
        await producer.handle_download_requested(event)

    assert recorder.failure_codes() == []
    assert not (storage.root / str(event.job_id)).exists()


async def test_a_failed_download_logs_the_reason_yt_dlp_gave(
    bus, storage, settings, recorder, caplog
) -> None:
    runner = ScriptedRunner(
        ProcessResult(1, "", "ERROR: [youtube] Sign in to confirm your age")
    )
    make_producer(bus, storage, runner, settings)
    event = download_request()

    with caplog.at_level("WARNING", logger="anything2telegram"):
        bus.emit("youtube.download.requested", event)
        await settle(bus)

    assert recorder.failure_codes() == ["youtube_process_failed"]
    logged = caplog.text
    assert str(event.job_id) in logged
    assert "exit=1" in logged
    assert "Sign in to confirm your age" in logged


async def test_yt_dlp_stderr_never_reaches_the_emitted_event(
    bus, storage, settings, recorder
) -> None:
    runner = ScriptedRunner(ProcessResult(1, "", "ERROR: cookies from /home/me/c.txt"))
    make_producer(bus, storage, runner, settings)

    bus.emit("youtube.download.requested", download_request())
    await settle(bus)

    assert recorder.production_failed[0].error.message == "YouTube process failed"
    assert "cookies" not in recorder.production_failed[0].error.message


async def test_an_unexpected_producer_crash_logs_a_traceback(
    bus, storage, settings, recorder, caplog
) -> None:
    make_producer(bus, storage, ScriptedRunner(RuntimeError("runner exploded")), settings)

    with caplog.at_level("WARNING", logger="anything2telegram"):
        bus.emit("youtube.download.requested", download_request())
        await settle(bus)

    assert recorder.failure_codes() == ["internal_error"]
    assert "runner exploded" in caplog.text
    assert "Traceback" in caplog.text


async def test_a_failed_playlist_expansion_logs_the_batch_and_reason(
    bus, storage, settings, recorder, caplog
) -> None:
    runner = ScriptedRunner(ProcessResult(1, "", "ERROR: playlist does not exist"))
    make_producer(bus, storage, runner, settings)
    event = expansion_request()

    with caplog.at_level("WARNING", logger="anything2telegram"):
        bus.emit("youtube.playlist.expansion.requested", event)
        await settle(bus)

    assert str(event.batch_id) in caplog.text
    assert "playlist does not exist" in caplog.text


@pytest.mark.parametrize(
    "caption,expected",
    [
        ("My Great Clip", "My Great Clip.mp4"),
        ("Rust - 03/12 - Traits", "Rust - 03_12 - Traits.mp4"),
        ("  .hidden ", "hidden.mp4"),
        ("..", "video.mp4"),
        ("é" * 200, "é" * 125 + ".mp4"),
    ],
)
def test_a_caption_becomes_a_filesystem_safe_filename(
    caption: str, expected: str
) -> None:
    name = _caption_filename(caption, ".mp4")

    assert name == expected
    assert len(name.encode()) <= 255


@pytest.mark.parametrize(
    "url,tab",
    [
        (
            "https://www.youtube.com/@Java.Brains",
            "https://www.youtube.com/@Java.Brains/playlists",
        ),
        (
            "https://www.youtube.com/@Java.Brains/videos",
            "https://www.youtube.com/@Java.Brains/playlists",
        ),
        (
            "https://www.youtube.com/channel/UC123abc",
            "https://www.youtube.com/channel/UC123abc/playlists",
        ),
        (
            "https://www.youtube.com/user/koushks",
            "https://www.youtube.com/user/koushks/playlists",
        ),
    ],
)
def test_a_channel_url_becomes_its_playlists_tab(url: str, tab: str) -> None:
    assert channel_playlists_url(url) == tab


def test_a_non_channel_url_has_no_playlists_tab() -> None:
    with pytest.raises(UnsupportedYouTubeUrl):
        channel_playlists_url("https://www.youtube.com/playlist?list=PL123")


CHANNEL = "https://www.youtube.com/@Java.Brains"


async def test_a_channels_playlists_are_listed_with_their_video_counts(
    bus, storage, settings
) -> None:
    runner = ScriptedRunner(
        ProcessResult(
            0,
            playlist_stdout(
                {"id": "PLspring", "title": "Spring Boot"},
                {"id": "PLdocker", "title": "Docker"},
            ),
            "",
        ),
        ProcessResult(0, playlist_stdout({"id": VIDEO_ID}, {"id": "bbbbbbbbbbb"}), ""),
        ProcessResult(0, playlist_stdout({"id": VIDEO_ID}), ""),
    )
    producer = make_producer(bus, storage, runner, settings)

    playlists = await producer.list_channel_playlists(CHANNEL)

    assert playlists == [
        ChannelPlaylist(
            "PLspring",
            "Spring Boot",
            "https://www.youtube.com/playlist?list=PLspring",
            2,
        ),
        ChannelPlaylist(
            "PLdocker", "Docker", "https://www.youtube.com/playlist?list=PLdocker", 1
        ),
    ]
    assert runner.calls[0][-1] == "https://www.youtube.com/@Java.Brains/playlists"
    assert "--flat-playlist" in runner.calls[0]
    assert [call[-1] for call in runner.calls[1:]] == [
        "https://www.youtube.com/playlist?list=PLspring",
        "https://www.youtube.com/playlist?list=PLdocker",
    ]


async def test_an_unreadable_line_costs_one_playlist_not_the_listing(
    bus, storage, settings
) -> None:
    runner = ScriptedRunner(
        ProcessResult(
            0,
            "not json\n"
            + playlist_stdout({"id": "PLspring", "title": "Spring Boot"})
            + "\n[]\n"
            + playlist_stdout({"id": "PLspring", "title": "Duplicate"}),
            "",
        ),
        ProcessResult(0, "", ""),
    )
    producer = make_producer(bus, storage, runner, settings)

    playlists = await producer.list_channel_playlists(CHANNEL)

    assert [playlist.playlist_id for playlist in playlists] == ["PLspring"]


async def test_a_playlist_without_a_title_falls_back_to_its_id(
    bus, storage, settings
) -> None:
    runner = ScriptedRunner(
        ProcessResult(0, playlist_stdout({"id": "PLspring"}), ""),
        ProcessResult(0, "", ""),
    )
    producer = make_producer(bus, storage, runner, settings)

    playlists = await producer.list_channel_playlists(CHANNEL)

    assert playlists[0].title == "PLspring"


@pytest.mark.parametrize(
    "failure",
    [ProcessResult(1, "", "channel does not exist"), ProcessTimeoutError()],
)
async def test_a_failed_channel_lookup_raises_rather_than_reporting_no_playlists(
    bus, storage, settings, failure: object
) -> None:
    producer = make_producer(bus, storage, ScriptedRunner(failure), settings)

    with pytest.raises(ChannelListingError):
        await producer.list_channel_playlists(CHANNEL)


async def test_a_failed_count_lookup_leaves_the_playlist_listed_with_zero(
    bus, storage, settings
) -> None:
    runner = ScriptedRunner(
        ProcessResult(0, playlist_stdout({"id": "PLspring", "title": "Spring"}), ""),
        ProcessTimeoutError(),
    )
    producer = make_producer(bus, storage, runner, settings)

    playlists = await producer.list_channel_playlists(CHANNEL)

    assert playlists[0].video_count == 0


async def test_a_playlists_widest_thumbnail_is_kept(bus, storage, settings) -> None:
    runner = ScriptedRunner(
        ProcessResult(
            0,
            playlist_stdout(
                {
                    "id": "PLspring",
                    "title": "Spring",
                    "thumbnails": [
                        {"url": "https://i.ytimg.com/vi/aaa/default.jpg", "width": 120},
                        {"url": "https://i.ytimg.com/vi/aaa/hq.jpg", "width": 480},
                        {"no_url": True},
                    ],
                }
            ),
            "",
        ),
        ProcessResult(0, "", ""),
    )
    producer = make_producer(bus, storage, runner, settings)

    playlists = await producer.list_channel_playlists(CHANNEL)

    assert playlists[0].thumbnail_url == "https://i.ytimg.com/vi/aaa/hq.jpg"


async def test_a_playlist_without_thumbnails_has_none(bus, storage, settings) -> None:
    runner = ScriptedRunner(
        ProcessResult(0, playlist_stdout({"id": "PLspring", "title": "Spring"}), ""),
        ProcessResult(0, "", ""),
    )
    producer = make_producer(bus, storage, runner, settings)

    playlists = await producer.list_channel_playlists(CHANNEL)

    assert playlists[0].thumbnail_url is None


async def test_an_expanded_playlist_carries_its_first_videos_thumbnail(
    bus, storage, settings, recorder
) -> None:
    runner = ScriptedRunner(
        ProcessResult(0, playlist_stdout({"id": VIDEO_ID}, {"id": "bbbbbbbbbbb"}), "")
    )
    make_producer(bus, storage, runner, settings)

    bus.emit("youtube.playlist.expansion.requested", expansion_request())
    await settle(bus)

    assert (
        recorder.expanded[0].playlist_thumbnail
        == f"https://i.ytimg.com/vi/{VIDEO_ID}/hqdefault.jpg"
    )
