import asyncio
from pathlib import Path

from httpx import ASGITransport, AsyncClient

from anything2telegram.config import Settings
from anything2telegram.domain import JobStatus
from anything2telegram.events import (
    ARTIFACT_UPLOAD_FAILED,
    TELEGRAM_UNAVAILABLE,
)
from anything2telegram.main import AdapterFactories, create_app
from anything2telegram.telegram.client import TelegramUnavailableError


class UnavailableTelegram:
    def __init__(self) -> None:
        self.connected = False
        self.upload_started = asyncio.Event()
        self.release_upload = asyncio.Event()
        self.disconnected = asyncio.Event()
        self.upload_calls = 0

    def is_connected(self) -> bool:
        return self.connected

    async def connect(self) -> None:
        self.connected = True

    async def disconnect(self) -> None:
        self.connected = False
        self.disconnected.set()

    async def wait_until_disconnected(self) -> None:
        await self.disconnected.wait()

    async def upload(self, _path: Path, **_kwargs: object):
        self.upload_calls += 1
        self.upload_started.set()
        await self.release_upload.wait()
        self.connected = False
        raise TelegramUnavailableError()


async def test_runtime_telegram_unavailable_pauses_choreography(
    tmp_path: Path,
) -> None:
    settings = Settings(
        api_id=1,
        api_hash="redacted",
        bot_token="redacted",
        channel_id=-1001,
        session_path=tmp_path / "service.session",
        cookies_path=None,
        artifact_root=tmp_path / "artifacts",
        max_artifact_bytes=1024,
        ytdlp_timeout_seconds=1,
        tg_upload_timeout_seconds=1,
        shutdown_grace_seconds=0.01,
    )
    telegram = UnavailableTelegram()
    service = create_app(
        settings,
        AdapterFactories(telegram_factory=lambda _settings: telegram),
    )
    transport = ASGITransport(app=service)
    async with AsyncClient(transport=transport, base_url="http://service") as client:
        outside = await client.get("/health")
        assert outside.status_code == 503
        assert outside.json() == {
            "ready": False,
            "telegram_connected": False,
        }

        async with service.router.lifespan_context(service):
            facts: list[str] = []
            unavailable = asyncio.Event()
            service.state.bus.on(
                ARTIFACT_UPLOAD_FAILED,
                lambda _event: facts.append(ARTIFACT_UPLOAD_FAILED),
            )

            def record_unavailable(_event: object) -> None:
                facts.append(TELEGRAM_UNAVAILABLE)
                unavailable.set()

            service.state.bus.on(TELEGRAM_UNAVAILABLE, record_unavailable)
            first = await client.post(
                "/jobs/upload",
                files={"file": ("first.mp4", b"first", "video/mp4")},
            )
            assert first.status_code == 202
            await asyncio.wait_for(telegram.upload_started.wait(), timeout=1)

            second = await client.post(
                "/jobs/upload",
                files={"file": ("second.mp4", b"second", "video/mp4")},
            )
            assert second.status_code == 202
            first_id = first.json()["id"]
            second_id = second.json()["id"]

            telegram.release_upload.set()
            await asyncio.wait_for(unavailable.wait(), timeout=1)
            await service.state.bus.wait_for_complete()
            await asyncio.sleep(0)

            current = await client.get(f"/jobs/{first_id}")
            queued = await client.get(f"/jobs/{second_id}")
            assert current.status_code == 200
            assert current.json()["status"] == JobStatus.FAILED.value
            assert queued.status_code == 200
            assert queued.json()["status"] == JobStatus.WAITING.value
            assert facts == [ARTIFACT_UPLOAD_FAILED, TELEGRAM_UNAVAILABLE]
            assert service.state.scheduler.paused is True
            assert service.state.scheduler.pending_count == 1
            assert telegram.upload_calls == 1

            rejected = await client.post(
                "/jobs/youtube",
                json={"url": "https://youtu.be/abcdefghijk"},
            )
            health = await client.get("/health")
            assert rejected.status_code == 503
            assert health.status_code == 503
            assert health.json() == {
                "ready": False,
                "telegram_connected": False,
            }
