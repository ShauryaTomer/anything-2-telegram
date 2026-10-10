"""Losing Telegram must stop the service admitting work instead of losing it."""

import asyncio

from httpx import ASGITransport, AsyncClient

from anything2telegram.config import Settings
from tests.conftest import FakeTelegram, FakeYouTubeRunner


VIDEO = "https://www.youtube.com/watch?v=AAAAAAAAAAA"


async def test_a_runtime_disconnect_makes_the_service_refuse_new_work(
    build_app,
) -> None:
    telegram = FakeTelegram()
    service = build_app(telegram=telegram)

    async with service.router.lifespan_context(service):
        transport = ASGITransport(app=service)
        async with AsyncClient(transport=transport, base_url="http://s") as client:
            assert (await client.get("/health")).status_code == 200

            telegram.drop()
            for _ in range(4):
                await asyncio.sleep(0)

            health = await client.get("/health")
            assert health.status_code == 503
            assert health.json() == {
                "ready": False,
                "database_connected": True,
                "telegram_connected": False,
                "scheduler_accepting": False,
            }

            refused = await client.post("/jobs/youtube", json={"url": VIDEO})
            assert refused.status_code == 503
            assert refused.json()["detail"]["code"] == "service_unavailable"

            upload = await client.post(
                "/jobs/upload", files={"file": ("clip.mp4", b"x", "video/mp4")}
            )
            assert upload.status_code == 503


async def test_an_upload_failure_pauses_the_queue_and_keeps_the_job_visible(
    build_app, settings: Settings
) -> None:
    runner = FakeYouTubeRunner()
    telegram = FakeTelegram()
    telegram.upload_errors = [ConnectionResetError("telegram vanished")]
    service = build_app(runner=runner, telegram=telegram)

    async with service.router.lifespan_context(service):
        transport = ASGITransport(app=service)
        async with AsyncClient(transport=transport, base_url="http://s") as client:
            job_id = (
                await client.post("/jobs/youtube", json={"url": VIDEO})
            ).json()["id"]

            job = None
            for _ in range(200):
                job = (await client.get(f"/jobs/{job_id}")).json()
                # JOB_QUEUED's tracker write is a scheduled task, not inline,
                # so the job can briefly 404 right after submission.
                if job.get("status") == "failed":
                    break
                await asyncio.sleep(0.01)

            assert job is not None and job["status"] == "failed"
            assert job["error"]["code"] == "internal_error"
            assert (await client.get(f"/jobs/{job_id}")).status_code == 200

    assert not (settings.artifact_root / job_id).exists()
