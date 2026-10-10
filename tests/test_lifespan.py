import asyncio
import logging
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest

from anything2telegram import main
from anything2telegram.config import Settings
from anything2telegram.domain import JobPhase, JobStatus, SourceKind
from anything2telegram.events import JobStarted
from anything2telegram.jobs import lifecycle
from anything2telegram.jobs.repositories import (
    JobQueueRepository,
    JobRow,
    JobsRepository,
    open_database,
)
from anything2telegram.jobs.scheduler import JobScheduler
from anything2telegram.jobs.tracker import JobTracker
from anything2telegram.telegram.client import TelegramUnavailableError
from tests.conftest import FakeTelegram, settings_for


async def test_startup_builds_every_component_and_opens_admission(
    build_app, settings: Settings
) -> None:
    telegram = FakeTelegram()
    service = build_app(telegram=telegram)

    async with service.router.lifespan_context(service):
        state = service.state
        assert state.settings is settings
        assert isinstance(state.tracker, JobTracker)
        assert isinstance(state.scheduler, JobScheduler)
        assert state.readiness.is_accepting() is True
        assert telegram.connected is True
        assert state.storage.root == settings.artifact_root

    assert telegram.connected is False


async def test_a_job_stuck_producing_or_uploading_is_interrupted_and_requeued(
    build_app, settings: Settings
) -> None:
    """A job caught mid-flight by a crash gets marked INTERRUPTED and retried.

    Pre-seeds the DB with a job in PRODUCING directly via the repository
    (simulating a crash), boots the real app through the lifespan, and
    asserts recovery ran before readiness opened.
    """
    job_id = uuid4()
    now = datetime.now(UTC)
    seed_conn = await open_database(settings.db_path)
    await JobsRepository(seed_conn).insert(
        JobRow(
            id=job_id,
            batch_id=None,
            source_kind=SourceKind.YOUTUBE,
            source="https://example.test/watch?v=stuck",
            title=None,
            status=JobStatus.PRODUCING,
            artifact_id=None,
            filename=None,
            size_bytes=None,
            telegram_chat_id=None,
            telegram_message_id=None,
            error=None,
            created_at=now,
            updated_at=now,
            staged=False,
        )
    )
    await seed_conn.close()

    service = build_app()
    async with service.router.lifespan_context(service):
        state = service.state
        snapshot = await state.tracker.get_job(job_id)
        assert snapshot is not None
        assert snapshot.status is JobStatus.INTERRUPTED

        queued = await JobQueueRepository(state.db).list_all()
        assert [row.job_id for row in queued] == [job_id]


async def test_recovered_youtube_job_clears_stale_artifact_when_restarted(
    settings: Settings,
) -> None:
    job_id = uuid4()
    artifact_id = uuid4()
    now = datetime.now(UTC)
    conn = await open_database(settings.db_path)
    jobs = JobsRepository(conn)
    queue = JobQueueRepository(conn)
    await jobs.insert(
        JobRow(
            id=job_id,
            batch_id=None,
            source_kind=SourceKind.YOUTUBE,
            source="https://example.test/watch?v=stuck",
            title="Interrupted upload",
            status=JobStatus.UPLOADING,
            artifact_id=artifact_id,
            filename="old.mp4",
            size_bytes=123,
            telegram_chat_id=None,
            telegram_message_id=None,
            error=None,
            created_at=now,
            updated_at=now,
            staged=False,
            ready_local_path=Path("/tmp/old.mp4"),
            ready_media_type="video/mp4",
            ready_caption="old",
        )
    )

    await main._recover_interrupted_jobs(jobs, queue)

    recovered = await jobs.get(job_id)
    assert recovered is not None
    assert recovered.status is JobStatus.INTERRUPTED
    lifecycle.apply(recovered, JobStarted(job_id, JobPhase.PRODUCING, now))
    assert recovered.artifact_id is None
    assert recovered.filename is None
    assert recovered.size_bytes is None
    assert recovered.ready_local_path is None
    assert recovered.ready_media_type is None
    assert recovered.ready_caption is None
    await conn.close()


async def test_settings_are_read_from_the_environment_when_not_supplied(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(main, "APPLICATION_BASE", tmp_path)
    monkeypatch.setattr(main, "YouTubeProcessRunner", lambda: object())
    monkeypatch.setattr(main, "TelegramClientAdapter", lambda _s: FakeTelegram())
    monkeypatch.setenv("TG_API_ID", "1")
    monkeypatch.setenv("TG_API_HASH", "hash")
    monkeypatch.setenv("TG_BOT_TOKEN", "token")
    monkeypatch.setenv("TG_CHANNEL_ID", "-1001")
    monkeypatch.setenv("ARTIFACT_ROOT", str(tmp_path / "artifacts"))

    service = main.create_app()
    async with service.router.lifespan_context(service):
        assert service.state.settings.api_id == 1


async def test_startup_clears_artifacts_left_behind_by_an_earlier_run(
    build_app, settings: Settings
) -> None:
    stale = settings.artifact_root / "leftover-job"
    stale.mkdir(parents=True)
    (stale / "clip.mp4").write_bytes(b"stale")

    service = build_app()
    async with service.router.lifespan_context(service):
        assert list(settings.artifact_root.iterdir()) == []


async def test_shutdown_clears_artifacts_and_disconnects(
    build_app, settings: Settings
) -> None:
    telegram = FakeTelegram()
    service = build_app(telegram=telegram)

    async with service.router.lifespan_context(service):
        leftover = settings.artifact_root / "in-flight-job"
        leftover.mkdir(parents=True)

    assert list(settings.artifact_root.iterdir()) == []
    assert telegram.connected is False


async def test_shutdown_stops_admitting_before_draining(build_app) -> None:
    service = build_app()

    async with service.router.lifespan_context(service):
        state = service.state
    assert state.readiness.is_accepting() is False
    assert state.scheduler.accepting is False


async def test_a_telegram_that_never_connects_aborts_startup(build_app) -> None:
    telegram = FakeTelegram()
    telegram.connect_error = TelegramUnavailableError()
    service = build_app(telegram=telegram)

    with pytest.raises(TelegramUnavailableError):
        async with service.router.lifespan_context(service):
            pass


async def test_an_event_handler_failure_closes_admission_and_fails_the_scheduler(
    build_app,
) -> None:
    service = build_app()

    async with service.router.lifespan_context(service):
        state = service.state
        state.bus.emit("error", RuntimeError("handler down"))

        assert state.readiness.is_accepting() is False
        assert state.scheduler.accepting is False


async def test_losing_telegram_at_runtime_closes_admission(build_app) -> None:
    telegram = FakeTelegram()
    service = build_app(telegram=telegram)

    async with service.router.lifespan_context(service):
        state = service.state
        telegram.drop()
        for _ in range(4):
            await asyncio.sleep(0)

        assert state.readiness.is_accepting() is False


async def test_telegram_reconnection_reopens_admission(build_app) -> None:
    telegram = FakeTelegram()
    service = build_app(telegram=telegram)

    async with service.router.lifespan_context(service):
        state = service.state
        telegram.drop()
        for _ in range(4):
            await asyncio.sleep(0)
        assert state.readiness.is_accepting() is False

        telegram.disconnected = asyncio.Event()
        telegram.connected = True
        await asyncio.sleep(1.1)

        assert state.readiness.is_accepting() is True
        assert state.scheduler.accepting is True


async def test_a_drain_that_overruns_its_grace_period_is_cancelled(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    impatient = settings_for(tmp_path, shutdown_grace_seconds=0.01)
    telegram = FakeTelegram()
    monkeypatch.setattr(main, "YouTubeProcessRunner", lambda: object())
    monkeypatch.setattr(main, "TelegramClientAdapter", lambda _s: telegram)
    service = main.create_app(impatient)

    async with service.router.lifespan_context(service):
        never_finishes = asyncio.Event()
        service.state.bus.on("slow", lambda _event: never_finishes.wait())
        service.state.bus.emit("slow", None)

    assert telegram.connected is False


async def test_a_handler_crash_logs_the_real_traceback(
    build_app, caplog: pytest.LogCaptureFixture
) -> None:
    service = build_app()

    async with service.router.lifespan_context(service):
        state = service.state

        async def explode(_event: object) -> None:
            raise RuntimeError("handler down")

        state.bus.on("boom", explode)
        with caplog.at_level("ERROR", logger="anything2telegram"):
            state.bus.emit("boom", None)
            for _ in range(4):
                await asyncio.sleep(0)

    # pyee re-emits from a done callback, so exc_info must be passed explicitly
    # or logging records "NoneType: None" instead of the failure.
    assert "handler down" in caplog.text
    assert "Traceback" in caplog.text
    assert "NoneType: None" not in caplog.text


async def test_the_configured_log_level_is_applied_to_our_package_only(
    build_app, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    quiet = settings_for(tmp_path, log_level="ERROR")
    monkeypatch.setattr(main, "YouTubeProcessRunner", lambda: object())
    monkeypatch.setattr(main, "TelegramClientAdapter", lambda _s: FakeTelegram())
    service = main.create_app(quiet)

    async with service.router.lifespan_context(service):
        assert logging.getLogger("anything2telegram").level == logging.ERROR
        assert logging.getLogger("telethon").level == logging.NOTSET
