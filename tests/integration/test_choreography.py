import asyncio
import json
from collections.abc import Sequence
from pathlib import Path
from uuid import UUID

from httpx import ASGITransport, AsyncClient

from anything2telegram.config import Settings
from anything2telegram.domain import ProcessResult, TelegramUploadResult
from anything2telegram.events import ARTIFACT_READY
from anything2telegram.main import AdapterFactories, create_app


class FakeYouTubeRunner:
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
        self, args: Sequence[str], _timeout_seconds: float | int
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
        output_template = Path(args[args.index("-o") + 1])
        output_template.parent.joinpath(f"{source_url[-11:]}.mp4").write_bytes(
            source_url.encode()
        )
        return ProcessResult(0, "", "")


class FakeTelegram:
    def __init__(self, *, manual_release: bool = False) -> None:
        self.connected = False
        self.disconnected = asyncio.Event()
        self.started = [asyncio.Event() for _ in range(8)]
        self.releases = [asyncio.Event() for _ in range(8)]
        if not manual_release:
            for release in self.releases:
                release.set()
        self.uploaded_filenames: list[str] = []
        self.active_uploads = 0
        self.max_active_uploads = 0

    def is_connected(self) -> bool:
        return self.connected

    async def connect(self) -> None:
        self.connected = True

    async def disconnect(self) -> None:
        self.connected = False
        self.disconnected.set()

    async def wait_until_disconnected(self) -> None:
        await self.disconnected.wait()

    async def upload(self, path: Path, **_kwargs: object) -> TelegramUploadResult:
        index = len(self.uploaded_filenames)
        self.uploaded_filenames.append(path.name)
        self.active_uploads += 1
        self.max_active_uploads = max(self.max_active_uploads, self.active_uploads)
        self.started[index].set()
        try:
            await self.releases[index].wait()
            return TelegramUploadResult(-1001, index + 1)
        finally:
            self.active_uploads -= 1


def settings_for(tmp_path: Path) -> Settings:
    return Settings(
        api_id=1,
        api_hash="unused",
        bot_token="unused",
        channel_id=-1001,
        session_path=tmp_path / "unused.session",
        cookies_path=None,
        artifact_root=tmp_path / "artifacts",
        max_artifact_bytes=4096,
        ytdlp_timeout_seconds=2,
        tg_upload_timeout_seconds=2,
        shutdown_grace_seconds=1,
    )


async def test_video_video_and_staged_upload_run_one_at_a_time_in_submit_order(
    tmp_path: Path,
) -> None:
    runner = FakeYouTubeRunner()
    telegram = FakeTelegram(manual_release=True)
    settings = settings_for(tmp_path)
    service = create_app(
        settings,
        AdapterFactories(
            process_runner_factory=lambda: runner,
            telegram_factory=lambda _settings: telegram,
        ),
    )
    transport = ASGITransport(app=service)

    async with service.router.lifespan_context(service):
        ready_job_ids = []
        service.state.bus.on(
            ARTIFACT_READY, lambda event: ready_job_ids.append(event.job_id)
        )
        async with AsyncClient(transport=transport, base_url="http://service") as client:
            first = await client.post(
                "/jobs/youtube", json={"url": "https://youtu.be/AAAAAAAAAAA"}
            )
            first_id = first.json()["id"]
            await asyncio.wait_for(telegram.started[0].wait(), timeout=1)

            second = await client.post(
                "/jobs/youtube", json={"url": "https://youtu.be/BBBBBBBBBBB"}
            )
            staged = await client.post(
                "/jobs/upload",
                files={"file": ("direct.mp4", b"direct", "video/mp4")},
            )
            second_id = second.json()["id"]
            staged_id = staged.json()["id"]
            first_uuid = UUID(first_id)
            second_uuid = UUID(second_id)
            staged_uuid = UUID(staged_id)

            assert first.status_code == second.status_code == staged.status_code == 202
            assert runner.calls == [("download", "https://youtu.be/AAAAAAAAAAA")]
            assert ready_job_ids == [first_uuid]
            assert service.state.scheduler.active_id == first_uuid
            assert service.state.scheduler.pending_count == 2
            assert (settings.artifact_root / staged_id).is_dir()

            telegram.releases[0].set()
            await asyncio.wait_for(telegram.started[1].wait(), timeout=1)
            assert telegram.uploaded_filenames == ["AAAAAAAAAAA.mp4", "BBBBBBBBBBB.mp4"]
            assert ready_job_ids == [
                first_uuid,
                second_uuid,
            ]
            assert (settings.artifact_root / first_id).exists() is False

            telegram.releases[1].set()
            await asyncio.wait_for(telegram.started[2].wait(), timeout=1)
            assert telegram.uploaded_filenames == [
                "AAAAAAAAAAA.mp4",
                "BBBBBBBBBBB.mp4",
                "direct.mp4",
            ]
            assert ready_job_ids == [
                first_uuid,
                second_uuid,
                staged_uuid,
            ]
            assert (settings.artifact_root / second_id).exists() is False

            telegram.releases[2].set()
            await service.state.bus.wait_for_complete()
            await asyncio.sleep(0)
            snapshots = [
                (await client.get(f"/jobs/{job_id}")).json()
                for job_id in (first_id, second_id, staged_id)
            ]
            assert [snapshot["status"] for snapshot in snapshots] == [
                "completed",
                "completed",
                "completed",
            ]
            assert telegram.max_active_uploads == 1
            assert all(
                not (settings.artifact_root / job_id).exists()
                for job_id in (first_id, second_id, staged_id)
            )


async def test_playlist_children_precede_later_video_and_derive_partial_batch(
    tmp_path: Path,
) -> None:
    child_one = "https://www.youtube.com/watch?v=AAAAAAAAAAA"
    child_two = "https://www.youtube.com/watch?v=CCCCCCCCCCC"
    standalone = "https://youtu.be/DDDDDDDDDDD"
    runner = FakeYouTubeRunner(
        playlist_entries=[
            {"id": "AAAAAAAAAAA", "availability": "public"},
            {"id": "AAAAAAAAAAA", "availability": "public"},
            {"id": "BBBBBBBBBBB", "availability": "unavailable"},
            {"id": "CCCCCCCCCCC", "availability": "public"},
        ],
        blocked_playlist=True,
        failed_urls={child_two},
    )
    telegram = FakeTelegram()
    settings = settings_for(tmp_path)
    service = create_app(
        settings,
        AdapterFactories(
            process_runner_factory=lambda: runner,
            telegram_factory=lambda _settings: telegram,
        ),
    )
    transport = ASGITransport(app=service)

    async with service.router.lifespan_context(service):
        async with AsyncClient(transport=transport, base_url="http://service") as client:
            playlist = await client.post(
                "/jobs/youtube",
                json={"url": "https://www.youtube.com/playlist?list=PL123"},
            )
            batch_id = playlist.json()["id"]
            await asyncio.wait_for(runner.playlist_started.wait(), timeout=1)
            later = await client.post("/jobs/youtube", json={"url": standalone})
            later_id = later.json()["id"]
            assert playlist.status_code == later.status_code == 202

            runner.release_playlist.set()
            for _ in range(100):
                batch = (await client.get(f"/batches/{batch_id}")).json()
                later_job = (await client.get(f"/jobs/{later_id}")).json()
                if batch["status"] == "partially_completed" and later_job[
                    "status"
                ] == "completed":
                    break
                await asyncio.sleep(0.01)

            child_jobs = [
                (await client.get(f"/jobs/{job_id}")).json()
                for job_id in batch["job_ids"]
            ]
            assert len(set(batch["job_ids"])) == 2
            assert [job["source"] for job in child_jobs] == [child_one, child_two]
            assert [job["batch_id"] for job in child_jobs] == [batch_id, batch_id]
            assert [job["status"] for job in child_jobs] == ["completed", "failed"]
            assert batch["status"] == "partially_completed"
            assert batch["skipped_entries"] == 2
            assert batch["total_jobs"] == 2
            assert runner.calls == [
                ("playlist", "https://www.youtube.com/playlist?list=PL123"),
                ("download", child_one),
                ("download", child_two),
                ("download", standalone),
            ]
            assert telegram.uploaded_filenames == [
                "AAAAAAAAAAA.mp4",
                "DDDDDDDDDDD.mp4",
            ]
            assert all(
                not (settings.artifact_root / job_id).exists()
                for job_id in (*batch["job_ids"], later_id)
            )
