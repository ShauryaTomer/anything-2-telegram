"""End-to-end: an HTTP submission travels the bus and reaches Telegram."""

import asyncio
from pathlib import Path

from httpx import ASGITransport, AsyncClient

from anything2telegram.config import Settings
from tests.conftest import FakeTelegram, FakeYouTubeRunner, settings_for


VIDEO_ONE = "https://www.youtube.com/watch?v=AAAAAAAAAAA"
VIDEO_TWO = "https://www.youtube.com/watch?v=CCCCCCCCCCC"
STANDALONE = "https://youtu.be/DDDDDDDDDDD"
PLAYLIST = "https://www.youtube.com/playlist?list=PL123"
PLAYLIST_META = {"playlist_title": "Rust Fundamentals", "playlist_count": 3}


async def poll_until(check, timeout: float = 2.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        result = await check()
        if result:
            return result
        await asyncio.sleep(0.01)
    raise AssertionError("condition never became true")


async def test_a_video_is_downloaded_uploaded_and_then_cleaned_up(
    build_app, settings: Settings
) -> None:
    runner = FakeYouTubeRunner()
    telegram = FakeTelegram()
    service = build_app(runner=runner, telegram=telegram)

    async with service.router.lifespan_context(service):
        transport = ASGITransport(app=service)
        async with AsyncClient(transport=transport, base_url="http://s") as client:
            submitted = await client.post("/jobs/youtube", json={"url": VIDEO_ONE})
            assert submitted.status_code == 202
            job_id = submitted.json()["id"]

            job = await poll_until(
                lambda: _snapshot_when(client, f"/jobs/{job_id}", "completed")
            )

    assert job["telegram_message_id"] == 1
    assert job["filename"] == "AAAAAAAAAAA.mp4"
    assert job["error"] is None
    assert telegram.uploaded_filenames == ["AAAAAAAAAAA.mp4"]
    assert runner.calls == [("download", VIDEO_ONE)]
    assert not (settings.artifact_root / job_id).exists()


async def test_jobs_run_one_at_a_time_in_submission_order(build_app) -> None:
    runner = FakeYouTubeRunner()
    telegram = FakeTelegram(manual_release=True)
    service = build_app(runner=runner, telegram=telegram)

    async with service.router.lifespan_context(service):
        transport = ASGITransport(app=service)
        async with AsyncClient(transport=transport, base_url="http://s") as client:
            first = (await client.post("/jobs/youtube", json={"url": VIDEO_ONE})).json()
            second = (
                await client.post("/jobs/youtube", json={"url": STANDALONE})
            ).json()

            await asyncio.wait_for(telegram.started[0].wait(), timeout=2)
            # The second job cannot have started while the first is uploading.
            assert telegram.uploaded_filenames == ["AAAAAAAAAAA.mp4"]
            second_job = (await client.get(f"/jobs/{second['id']}")).json()
            assert second_job["status"] == "waiting"

            telegram.releases[0].set()
            telegram.releases[1].set()
            await poll_until(
                lambda: _snapshot_when(client, f"/jobs/{second['id']}", "completed")
            )

    assert telegram.uploaded_filenames == ["AAAAAAAAAAA.mp4", "DDDDDDDDDDD.mp4"]
    assert telegram.max_active_uploads == 1
    assert first["id"] != second["id"]


async def test_a_playlist_expands_and_its_children_run_before_later_work(
    build_app, settings: Settings
) -> None:
    runner = FakeYouTubeRunner(
        playlist_entries=[
            {"id": "AAAAAAAAAAA", **PLAYLIST_META, "playlist_index": 1},
            {"id": "AAAAAAAAAAA", **PLAYLIST_META, "playlist_index": 2},
            {"id": "CCCCCCCCCCC", **PLAYLIST_META, "playlist_index": 3},
        ],
        blocked_playlist=True,
        failed_urls={VIDEO_TWO},
    )
    telegram = FakeTelegram()
    service = build_app(runner=runner, telegram=telegram)

    async with service.router.lifespan_context(service):
        transport = ASGITransport(app=service)
        async with AsyncClient(transport=transport, base_url="http://s") as client:
            batch_id = (
                await client.post("/jobs/youtube", json={"url": PLAYLIST})
            ).json()["id"]
            await asyncio.wait_for(runner.playlist_started.wait(), timeout=2)
            later_id = (
                await client.post("/jobs/youtube", json={"url": STANDALONE})
            ).json()["id"]

            runner.release_playlist.set()
            batch = await poll_until(
                lambda: _snapshot_when(
                    client, f"/batches/{batch_id}", "partially_completed"
                )
            )
            await poll_until(
                lambda: _snapshot_when(client, f"/jobs/{later_id}", "completed")
            )
            children = [
                (await client.get(f"/jobs/{job_id}")).json()
                for job_id in batch["job_ids"]
            ]

    assert batch["total_jobs"] == 2
    assert batch["skipped_entries"] == 1
    assert [child["source"] for child in children] == [VIDEO_ONE, VIDEO_TWO]
    assert [child["status"] for child in children] == ["completed", "failed"]
    assert all(child["batch_id"] == batch_id for child in children)
    assert children[1]["error"]["code"] == "youtube_process_failed"
    # The playlist name is posted once, and each child caption carries its number.
    assert telegram.messages == ["Rust Fundamentals - 2 video(s)"]
    assert telegram.captions == [
        "Rust Fundamentals - 1/3 - AAAAAAAAAAA",
        "DDDDDDDDDDD",
    ]
    # Playlist children jump ahead of the video submitted while it was expanding.
    assert runner.calls == [
        ("playlist", PLAYLIST),
        ("download", VIDEO_ONE),
        ("download", VIDEO_TWO),
        ("download", STANDALONE),
    ]
    assert all(
        not (settings.artifact_root / job_id).exists()
        for job_id in (*batch["job_ids"], later_id)
    )


async def test_an_empty_playlist_fails_the_batch(build_app) -> None:
    runner = FakeYouTubeRunner(playlist_entries=[])
    service = build_app(runner=runner)

    async with service.router.lifespan_context(service):
        transport = ASGITransport(app=service)
        async with AsyncClient(transport=transport, base_url="http://s") as client:
            batch_id = (
                await client.post("/jobs/youtube", json={"url": PLAYLIST})
            ).json()["id"]

            batch = await poll_until(
                lambda: _snapshot_when(client, f"/batches/{batch_id}", "failed")
            )

    assert batch["error"]["code"] == "playlist_empty"
    assert batch["job_ids"] == []


async def test_an_uploaded_file_travels_from_the_form_to_telegram(
    build_app, settings: Settings
) -> None:
    telegram = FakeTelegram()
    service = build_app(telegram=telegram)

    async with service.router.lifespan_context(service):
        transport = ASGITransport(app=service)
        async with AsyncClient(transport=transport, base_url="http://s") as client:
            submitted = await client.post(
                "/jobs/upload",
                files={"file": ("holiday clip.mp4", b"payload", "video/mp4")},
                data={"caption": "from my phone"},
            )
            assert submitted.status_code == 202
            job_id = submitted.json()["id"]

            job = await poll_until(
                lambda: _snapshot_when(client, f"/jobs/{job_id}", "completed")
            )

    assert job["source_kind"] == "local_upload"
    assert job["filename"] == "holiday_clip.mp4"
    assert job["size_bytes"] == 7
    assert telegram.uploaded_filenames == ["holiday_clip.mp4"]
    assert telegram.captions == ["from my phone"]
    assert not (settings.artifact_root / job_id).exists()


async def test_a_download_that_outgrows_the_limit_fails_the_job(
    build_app, tmp_path: Path
) -> None:
    small = settings_for(tmp_path, max_artifact_bytes=2)
    runner = FakeYouTubeRunner()
    service = build_app(runner=runner, app_settings=small)

    async with service.router.lifespan_context(service):
        transport = ASGITransport(app=service)
        async with AsyncClient(transport=transport, base_url="http://s") as client:
            job_id = (
                await client.post("/jobs/youtube", json={"url": VIDEO_ONE})
            ).json()["id"]

            job = await poll_until(
                lambda: _snapshot_when(client, f"/jobs/{job_id}", "failed")
            )

    assert job["error"]["code"] == "artifact_oversize"
    assert not (small.artifact_root / job_id).exists()


async def _snapshot_when(client: AsyncClient, path: str, status: str):
    response = await client.get(path)
    if response.status_code != 200:
        return None
    body = response.json()
    return body if body["status"] == status else None
