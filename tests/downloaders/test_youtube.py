import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from pyee.asyncio import AsyncIOEventEmitter

from anything2telegram.artifacts.storage import ArtifactStorage
from anything2telegram.downloaders.process import ProcessResult, ProcessTimeoutError
from anything2telegram.downloaders.youtube import (
    YTDLP_FORMAT,
    UnsupportedYouTubeUrl,
    YouTubeArtifactProducer,
    YouTubeUrlKind,
    classify_youtube_url,
)
from anything2telegram.events import (
    ARTIFACT_PRODUCTION_FAILED,
    ARTIFACT_READY,
    YOUTUBE_DOWNLOAD_REQUESTED,
    YOUTUBE_PLAYLIST_EXPANDED,
    YOUTUBE_PLAYLIST_EXPANSION_FAILED,
    YOUTUBE_PLAYLIST_EXPANSION_REQUESTED,
    PlaylistExpansionRequested,
    YouTubeDownloadRequested,
)


NOW = datetime(2026, 7, 19, tzinfo=UTC)
ARTIFACT_ID = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")


class Runner:
    def __init__(self, result=None, error=None, side_effect=None) -> None:
        self.result = result or ProcessResult(0, "", "")
        self.error = error
        self.side_effect = side_effect
        self.calls = []

    async def run(self, args, timeout_seconds):
        self.calls.append((list(args), timeout_seconds))
        if self.side_effect:
            self.side_effect(args)
        if self.error:
            raise self.error
        return self.result


async def settle() -> None:
    for _ in range(5):
        await asyncio.sleep(0)


@pytest.mark.parametrize(
    ("url", "kind"),
    [
        ("https://youtube.com/watch?v=dQw4w9WgXcQ", YouTubeUrlKind.VIDEO),
        ("https://music.youtube.com/watch?v=dQw4w9WgXcQ", YouTubeUrlKind.VIDEO),
        ("https://youtu.be/dQw4w9WgXcQ", YouTubeUrlKind.VIDEO),
        ("https://youtube.com/shorts/dQw4w9WgXcQ", YouTubeUrlKind.VIDEO),
        ("https://youtube.com/watch?v=dQw4w9WgXcQ&list=PL123", YouTubeUrlKind.VIDEO),
        ("https://youtube.com/playlist?list=PL123", YouTubeUrlKind.PLAYLIST),
        ("https://youtube.com/?list=PL123", YouTubeUrlKind.PLAYLIST),
    ],
)
def test_classifies_supported_youtube_urls(url, kind) -> None:
    assert classify_youtube_url(url) is kind


@pytest.mark.parametrize(
    "url",
    [
        "https://youtube.com.evil.test/watch?v=dQw4w9WgXcQ",
        "https://notyoutube.com/watch?v=dQw4w9WgXcQ",
        "https://youtube.com@evil.test/watch?v=dQw4w9WgXcQ",
        "https://user@youtube.com/watch?v=dQw4w9WgXcQ",
        "ftp://youtube.com/watch?v=dQw4w9WgXcQ",
        "https://youtube.com:443/watch?v=dQw4w9WgXcQ",
        "https://youtube.com/channel/UC123",
        "https://youtu.be/not-an-id/extra",
    ],
)
def test_rejects_deceptive_or_unsupported_urls(url) -> None:
    with pytest.raises(UnsupportedYouTubeUrl) as caught:
        classify_youtube_url(url)
    assert caught.value.code == "unsupported_youtube_url"
    assert url not in str(caught.value)


@pytest.mark.asyncio
async def test_expansion_uses_exact_args_and_emits_ordered_deduped_targets(tmp_path) -> None:
    output = "\n".join(
        json.dumps(entry)
        for entry in [
            {"id": "dQw4w9WgXcQ"},
            {"id": "M7lc1UVf-VE", "availability": "private"},
            {"id": "9bZkp7q19f0"},
            {"id": "dQw4w9WgXcQ"},
            {"title": "malformed"},
        ]
    )
    runner = Runner(ProcessResult(0, output, ""))
    bus = AsyncIOEventEmitter()
    errors = []
    expanded = []
    bus.on("error", errors.append)
    bus.on(YOUTUBE_PLAYLIST_EXPANDED, expanded.append)
    YouTubeArtifactProducer(
        bus,
        ArtifactStorage(tmp_path / "artifacts"),
        runner,
        timeout_seconds=15,
        cookies_path=tmp_path / "cookies.txt",
        id_factory=lambda: ARTIFACT_ID,
        clock=lambda: NOW,
    )
    request = PlaylistExpansionRequested(uuid4(), "https://youtube.com/playlist?list=PL123", NOW)
    bus.emit(YOUTUBE_PLAYLIST_EXPANSION_REQUESTED, request)
    await settle()

    assert errors == []
    assert runner.calls == [
        ([
            "yt-dlp", "--flat-playlist", "--dump-json", "--no-warnings",
            "--ignore-errors", "--cookies", str(tmp_path / "cookies.txt"),
            request.source_url,
        ], 15)
    ]
    assert [target.source_id for target in expanded[0].targets] == [
        "dQw4w9WgXcQ", "9bZkp7q19f0"
    ]
    assert expanded[0].skipped_entries == 3
    assert expanded[0].occurred_at == NOW


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("result", "code"),
    [
        (ProcessResult(0, "", ""), "playlist_empty"),
        (ProcessResult(0, "not-json", ""), "playlist_parse_failed"),
        (ProcessResult(2, "", "safe"), "youtube_process_failed"),
    ],
)
async def test_expansion_converts_empty_malformed_and_process_failures(tmp_path, result, code) -> None:
    runner = Runner(result)
    bus = AsyncIOEventEmitter()
    failures = []
    bus.on("error", lambda error: pytest.fail(str(error)))
    bus.on(YOUTUBE_PLAYLIST_EXPANSION_FAILED, failures.append)
    YouTubeArtifactProducer(bus, ArtifactStorage(tmp_path / "root"), runner, timeout_seconds=1)
    request = PlaylistExpansionRequested(uuid4(), "https://youtube.com/playlist?list=PL123", NOW)
    bus.emit(YOUTUBE_PLAYLIST_EXPANSION_REQUESTED, request)
    await settle()
    assert failures[0].batch_id == request.batch_id
    assert failures[0].error.code == code
    assert request.source_url not in failures[0].error.message


@pytest.mark.asyncio
async def test_download_exact_args_and_ready_metadata(tmp_path) -> None:
    storage = ArtifactStorage(tmp_path / "root")

    def create_output(args):
        template = Path(args[args.index("-o") + 1])
        (template.parent / "My_Video.mp4").write_bytes(b"video")
        (template.parent / "My_Video.info.json").write_text("{}")
        (template.parent / "My_Video.mp4.part").write_bytes(b"partial")

    runner = Runner(side_effect=create_output)
    bus = AsyncIOEventEmitter()
    ready = []
    bus.on("error", lambda error: pytest.fail(str(error)))
    bus.on(ARTIFACT_READY, ready.append)
    YouTubeArtifactProducer(
        bus,
        storage,
        runner,
        timeout_seconds=22,
        max_artifact_bytes=20,
        id_factory=lambda: ARTIFACT_ID,
        clock=lambda: NOW,
    )
    event = YouTubeDownloadRequested(uuid4(), "https://youtu.be/dQw4w9WgXcQ", NOW)
    bus.emit(YOUTUBE_DOWNLOAD_REQUESTED, event)
    await settle()

    directory = storage.root / str(event.job_id) / str(ARTIFACT_ID)
    assert runner.calls[0] == ([
        "yt-dlp", "-f", YTDLP_FORMAT, "--merge-output-format", "mp4",
        "--max-filesize", "20",
        "--restrict-filenames", "--no-playlist", "--js-runtimes", "node",
        "--remote-components", "ejs:github", "-o",
        str(directory / "%(title).80s.%(ext)s"), event.source_url,
    ], 22)
    assert ready[0].local_path == directory / "My_Video.mp4"
    assert ready[0].filename == "My_Video.mp4"
    assert ready[0].caption == "My Video"
    assert ready[0].media_type == "video/mp4"
    assert ready[0].size_bytes == 5


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["missing", "ambiguous", "symlink"])
async def test_download_rejects_unsafe_or_ambiguous_outputs_and_cleans(tmp_path, mode) -> None:
    storage = ArtifactStorage(tmp_path / "root")
    job_id = uuid4()

    def create_output(args):
        directory = Path(args[args.index("-o") + 1]).parent
        if mode == "ambiguous":
            (directory / "one.mp4").write_bytes(b"1")
            (directory / "two.webm").write_bytes(b"2")
        elif mode == "symlink":
            outside = tmp_path / "outside.mp4"
            outside.write_bytes(b"x")
            (directory / "video.mp4").symlink_to(outside)

    runner = Runner(side_effect=create_output)
    bus = AsyncIOEventEmitter()
    ready, failed = [], []
    bus.on("error", lambda error: pytest.fail(str(error)))
    bus.on(ARTIFACT_READY, ready.append)
    bus.on(ARTIFACT_PRODUCTION_FAILED, failed.append)
    YouTubeArtifactProducer(bus, storage, runner, timeout_seconds=1, id_factory=lambda: ARTIFACT_ID)
    bus.emit(YOUTUBE_DOWNLOAD_REQUESTED, YouTubeDownloadRequested(job_id, "https://youtu.be/dQw4w9WgXcQ", NOW))
    await settle()
    assert ready == []
    assert failed[0].error.code == "internal_error"
    assert not (storage.root / str(job_id)).exists()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "result", "code", "oversize"),
    [
        (ProcessTimeoutError(), None, "youtube_timeout", False),
        (None, ProcessResult(3, "", "safe"), "youtube_process_failed", False),
        (RuntimeError("URL and secret"), None, "internal_error", False),
        (None, None, "artifact_oversize", True),
    ],
)
async def test_download_converts_errors_and_cleans(
    tmp_path, error, result, code, oversize
) -> None:
    storage = ArtifactStorage(tmp_path / "root")
    cancelled = asyncio.Event()

    class OversizeRunner:
        async def run(self, args, timeout_seconds):
            directory = Path(args[args.index("-o") + 1]).parent
            (directory / "growing.part").write_bytes(b"12345")
            try:
                await asyncio.Future()
            finally:
                cancelled.set()

    runner = OversizeRunner() if oversize else Runner(result=result, error=error)
    bus = AsyncIOEventEmitter()
    failures, ready = [], []
    bus.on("error", lambda value: pytest.fail(str(value)))
    bus.on(ARTIFACT_PRODUCTION_FAILED, failures.append)
    bus.on(ARTIFACT_READY, ready.append)
    job_id = uuid4()
    YouTubeArtifactProducer(
        bus,
        storage,
        runner,
        timeout_seconds=1,
        max_artifact_bytes=4,
        id_factory=lambda: ARTIFACT_ID,
    )
    bus.emit(YOUTUBE_DOWNLOAD_REQUESTED, YouTubeDownloadRequested(job_id, "https://youtu.be/dQw4w9WgXcQ", NOW))
    if oversize:
        await asyncio.wait_for(cancelled.wait(), 0.1)
    await settle()
    assert ready == []
    assert len(failures) == 1
    assert failures[0].artifact_id == ARTIFACT_ID
    assert failures[0].error.code == code
    assert "secret" not in failures[0].error.message.lower()
    assert not (storage.root / str(job_id)).exists()


@pytest.mark.asyncio
async def test_download_cancellation_cleans_and_emits_no_failure(tmp_path) -> None:
    storage = ArtifactStorage(tmp_path / "root")
    started = asyncio.Event()

    class BlockingRunner:
        async def run(self, args, timeout_seconds):
            started.set()
            await asyncio.Future()

    bus = AsyncIOEventEmitter()
    failures = []
    bus.on("error", lambda error: None)
    bus.on(ARTIFACT_PRODUCTION_FAILED, failures.append)
    producer = YouTubeArtifactProducer(bus, storage, BlockingRunner(), timeout_seconds=1)
    event = YouTubeDownloadRequested(uuid4(), "https://youtu.be/dQw4w9WgXcQ", NOW)
    task = asyncio.create_task(producer.handle_download_requested(event))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert failures == []
    assert not (storage.root / str(event.job_id)).exists()


@pytest.mark.asyncio
async def test_download_base_exception_cleans_and_reraises(tmp_path) -> None:
    class Shutdown(BaseException):
        pass

    class ShutdownRunner:
        async def run(self, args, timeout_seconds):
            raise Shutdown()

    storage = ArtifactStorage(tmp_path / "root")
    bus = AsyncIOEventEmitter()
    failures = []
    bus.on(ARTIFACT_PRODUCTION_FAILED, failures.append)
    producer = YouTubeArtifactProducer(bus, storage, ShutdownRunner(), timeout_seconds=1)
    event = YouTubeDownloadRequested(uuid4(), "https://youtu.be/dQw4w9WgXcQ", NOW)

    with pytest.raises(Shutdown):
        await producer.handle_download_requested(event)
    assert failures == []
    assert not (storage.root / str(event.job_id)).exists()


@pytest.mark.asyncio
async def test_download_factory_error_emits_one_correlated_failure_without_bus_error(
    tmp_path,
) -> None:
    def fail_id_factory():
        raise RuntimeError("factory secret")

    bus = AsyncIOEventEmitter()
    bus_errors = []
    failures = []
    bus.on("error", bus_errors.append)
    bus.on(ARTIFACT_PRODUCTION_FAILED, failures.append)
    YouTubeArtifactProducer(
        bus,
        ArtifactStorage(tmp_path / "root"),
        Runner(),
        timeout_seconds=1,
        id_factory=fail_id_factory,
    )
    event = YouTubeDownloadRequested(
        uuid4(), "https://youtu.be/dQw4w9WgXcQ", NOW
    )

    bus.emit(YOUTUBE_DOWNLOAD_REQUESTED, event)
    await settle()

    assert bus_errors == []
    assert len(failures) == 1
    assert failures[0].job_id == event.job_id
    assert failures[0].artifact_id is None
    assert failures[0].error.code == "internal_error"
