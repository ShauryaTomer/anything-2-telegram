import asyncio
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from pyee import EventEmitter

from anything2telegram.artifacts.storage import ArtifactStorage
from anything2telegram.domain import ErrorInfo, JobPhase
from anything2telegram.events import (
    ARTIFACT_PRODUCTION_FAILED,
    ARTIFACT_READY,
    ARTIFACT_UPLOAD_FAILED,
    ARTIFACT_UPLOADED,
    BATCH_CREATED,
    BATCH_JOBS_CREATED,
    ERROR,
    JOB_QUEUED,
    JOB_STARTED,
    TELEGRAM_UNAVAILABLE,
    YOUTUBE_DOWNLOAD_REQUESTED,
    YOUTUBE_PLAYLIST_EXPANDED,
    YOUTUBE_PLAYLIST_EXPANSION_FAILED,
    YOUTUBE_PLAYLIST_EXPANSION_REQUESTED,
    ArtifactProductionFailed,
    ArtifactReady,
    ArtifactUploaded,
    ArtifactUploadFailed,
    DownloadTarget,
    PlaylistExpanded,
    PlaylistExpansionFailed,
    TelegramUnavailable,
)
from anything2telegram.jobs.scheduler import JobScheduler, SchedulerError


def _stamp() -> datetime:
    return datetime.now(UTC)


VIDEO = "https://www.youtube.com/watch?v=aaaaaaaaaaa"
OTHER_VIDEO = "https://www.youtube.com/watch?v=bbbbbbbbbbb"
PLAYLIST = "https://www.youtube.com/playlist?list=PL123"

_TOPICS = (
    BATCH_CREATED,
    BATCH_JOBS_CREATED,
    JOB_QUEUED,
    JOB_STARTED,
    YOUTUBE_DOWNLOAD_REQUESTED,
    YOUTUBE_PLAYLIST_EXPANSION_REQUESTED,
    ARTIFACT_READY,
    ARTIFACT_UPLOAD_FAILED,
)


class Recorder:
    """Captures every fact the scheduler publishes, in order."""

    def __init__(self, bus: EventEmitter) -> None:
        self.facts: list[tuple[str, object]] = []
        for topic in _TOPICS:
            bus.on(topic, self._make_listener(topic))

    def _make_listener(self, topic: str):
        def listen(event: object) -> None:
            self.facts.append((topic, event))

        return listen

    def topics(self) -> list[str]:
        return [topic for topic, _ in self.facts]

    def only(self, topic: str) -> list[object]:
        return [event for name, event in self.facts if name == topic]


@pytest.fixture
def bus() -> EventEmitter:
    return EventEmitter()


@pytest.fixture
def recorder(bus: EventEmitter) -> Recorder:
    return Recorder(bus)


@pytest.fixture
def storage(tmp_path: Path) -> ArtifactStorage:
    return ArtifactStorage(tmp_path / "artifacts")


@pytest.fixture
def scheduler(bus: EventEmitter, storage: ArtifactStorage) -> JobScheduler:
    return JobScheduler(bus, storage)


async def settle() -> None:
    """Let the pump's call_soon callbacks run."""
    for _ in range(4):
        await asyncio.sleep(0)


async def stage_upload(
    scheduler: JobScheduler, storage: ArtifactStorage, payload: bytes = b"abc"
):
    reservation = scheduler.reserve_local_upload("clip.mp4", "video/mp4", "caption")
    reservation.destination.write_bytes(payload)
    return reservation


async def test_a_submitted_video_is_queued_then_started_and_requested(
    scheduler: JobScheduler, recorder: Recorder
) -> None:
    ref = scheduler.submit_video(VIDEO)
    assert ref.status_url == f"/jobs/{ref.id}"
    assert recorder.topics() == [JOB_QUEUED]

    await settle()

    assert recorder.topics() == [JOB_QUEUED, JOB_STARTED, YOUTUBE_DOWNLOAD_REQUESTED]
    queued, started, requested = (event for _, event in recorder.facts)
    assert (queued.job_id, queued.source, queued.batch_id) == (ref.id, VIDEO, None)
    assert started.phase is JobPhase.PRODUCING
    assert requested.source_url == VIDEO
    assert queued.occurred_at <= started.occurred_at <= requested.occurred_at


async def test_only_one_job_runs_until_the_first_one_finishes(
    scheduler: JobScheduler, bus: EventEmitter, recorder: Recorder
) -> None:
    first = scheduler.submit_video(VIDEO)
    second = scheduler.submit_video(OTHER_VIDEO)
    await settle()

    assert [event.source_url for event in recorder.only(YOUTUBE_DOWNLOAD_REQUESTED)] == [
        VIDEO
    ]

    artifact_id = uuid4()
    bus.emit(
        ARTIFACT_READY,
        ArtifactReady(
            first.id, artifact_id, Path("/tmp/a.mp4"), "a.mp4", None, 3, None, _stamp()
        ),
    )
    bus.emit(
        ARTIFACT_UPLOADED, ArtifactUploaded(first.id, artifact_id, -1001, 7, _stamp())
    )
    await settle()

    assert [event.source_url for event in recorder.only(YOUTUBE_DOWNLOAD_REQUESTED)] == [
        VIDEO,
        OTHER_VIDEO,
    ]
    assert recorder.only(JOB_STARTED)[-1].job_id == second.id


async def test_a_production_failure_also_releases_the_slot(
    scheduler: JobScheduler, bus: EventEmitter, recorder: Recorder
) -> None:
    first = scheduler.submit_video(VIDEO)
    scheduler.submit_video(OTHER_VIDEO)
    await settle()

    bus.emit(
        ARTIFACT_PRODUCTION_FAILED,
        ArtifactProductionFailed(
            first.id, None, ErrorInfo("youtube_timeout", "Timed out"), _stamp()
        ),
    )
    await settle()

    assert len(recorder.only(YOUTUBE_DOWNLOAD_REQUESTED)) == 2


async def test_a_terminal_event_for_another_job_is_ignored(
    scheduler: JobScheduler, bus: EventEmitter, recorder: Recorder
) -> None:
    scheduler.submit_video(VIDEO)
    scheduler.submit_video(OTHER_VIDEO)
    await settle()

    bus.emit(
        ARTIFACT_PRODUCTION_FAILED,
        ArtifactProductionFailed(
            uuid4(), None, ErrorInfo("boom", "Boom"), _stamp()
        ),
    )
    await settle()

    assert len(recorder.only(YOUTUBE_DOWNLOAD_REQUESTED)) == 1


async def test_an_upload_outcome_for_a_stale_artifact_is_ignored(
    scheduler: JobScheduler, bus: EventEmitter, recorder: Recorder
) -> None:
    first = scheduler.submit_video(VIDEO)
    scheduler.submit_video(OTHER_VIDEO)
    await settle()
    bus.emit(
        ARTIFACT_READY,
        ArtifactReady(
            first.id, uuid4(), Path("/tmp/a.mp4"), "a.mp4", None, 3, None, _stamp()
        ),
    )

    bus.emit(
        ARTIFACT_UPLOAD_FAILED,
        ArtifactUploadFailed(
            first.id, uuid4(), ErrorInfo("boom", "Boom"), _stamp()
        ),
    )
    await settle()

    assert len(recorder.only(YOUTUBE_DOWNLOAD_REQUESTED)) == 1


async def test_a_playlist_expands_into_children_that_run_before_later_work(
    scheduler: JobScheduler, bus: EventEmitter, recorder: Recorder
) -> None:
    batch = scheduler.submit_playlist(PLAYLIST, 2)
    later = scheduler.submit_video(OTHER_VIDEO)
    await settle()

    requested = recorder.only(YOUTUBE_PLAYLIST_EXPANSION_REQUESTED)
    assert [(event.batch_id, event.offset) for event in requested] == [(batch.id, 2)]

    bus.emit(
        YOUTUBE_PLAYLIST_EXPANDED,
        PlaylistExpanded(
            batch.id,
            (DownloadTarget("aaaaaaaaaaa", VIDEO),),
            3,
            _stamp(),
        ),
    )
    await settle()

    created = recorder.only(BATCH_JOBS_CREATED)
    assert len(created) == 1
    assert created[0].skipped_entries == 3
    child_id = created[0].job_ids[0]
    child_queued = [
        event for event in recorder.only(JOB_QUEUED) if event.batch_id == batch.id
    ]
    assert [event.job_id for event in child_queued] == [child_id]
    # The child jumps the queue ahead of the video submitted after the playlist.
    assert [event.job_id for event in recorder.only(JOB_STARTED)] == [child_id]
    assert later.id not in {event.job_id for event in recorder.only(JOB_STARTED)}


async def test_a_failed_expansion_releases_the_slot(
    scheduler: JobScheduler, bus: EventEmitter, recorder: Recorder
) -> None:
    batch = scheduler.submit_playlist(PLAYLIST)
    scheduler.submit_video(VIDEO)
    await settle()

    bus.emit(
        YOUTUBE_PLAYLIST_EXPANSION_FAILED,
        PlaylistExpansionFailed(
            batch.id, ErrorInfo("playlist_empty", "Empty"), _stamp()
        ),
    )
    await settle()

    assert len(recorder.only(YOUTUBE_DOWNLOAD_REQUESTED)) == 1


async def test_a_reserved_upload_becomes_a_ready_artifact(
    scheduler: JobScheduler, storage: ArtifactStorage, recorder: Recorder
) -> None:
    reservation = await stage_upload(scheduler, storage)

    ref = scheduler.enqueue_reserved_upload(reservation, 3)
    await settle()

    assert recorder.topics() == [JOB_QUEUED, JOB_STARTED, ARTIFACT_READY]
    queued, started, ready = (event for _, event in recorder.facts)
    assert queued.source == "clip.mp4"
    assert queued.staged_artifact is not None
    assert started.phase is JobPhase.UPLOADING
    assert (ready.job_id, ready.size_bytes, ready.caption) == (ref.id, 3, "caption")
    assert ready.local_path == reservation.destination


async def test_an_unknown_or_mismatched_reservation_is_refused(
    scheduler: JobScheduler, storage: ArtifactStorage
) -> None:
    reservation = await stage_upload(scheduler, storage)
    scheduler.enqueue_reserved_upload(reservation, 3)

    with pytest.raises(SchedulerError) as error:
        scheduler.enqueue_reserved_upload(reservation, 3)
    assert error.value.code == "invalid_reservation"


async def test_a_reservation_whose_file_never_arrived_is_refused(
    scheduler: JobScheduler,
) -> None:
    reservation = scheduler.reserve_local_upload("clip.mp4", None, None)

    with pytest.raises(SchedulerError) as error:
        scheduler.enqueue_reserved_upload(reservation, 3)
    assert error.value.code == "upload_not_staged"


async def test_an_artifact_that_vanished_before_upload_fails_the_job(
    scheduler: JobScheduler, storage: ArtifactStorage, recorder: Recorder
) -> None:
    reservation = await stage_upload(scheduler, storage)
    ref = scheduler.enqueue_reserved_upload(reservation, 3)
    reservation.destination.unlink()

    await settle()

    failures = recorder.only(ARTIFACT_UPLOAD_FAILED)
    assert [event.error.code for event in failures] == ["artifact_invalid"]
    assert recorder.only(ARTIFACT_READY) == []
    assert not (storage.root / str(ref.id)).exists()


async def test_cancelling_a_reservation_deletes_its_directory(
    scheduler: JobScheduler, storage: ArtifactStorage
) -> None:
    reservation = await stage_upload(scheduler, storage)

    assert scheduler.cancel_reserved_upload(reservation.job_id) is True
    assert not (storage.root / str(reservation.job_id)).exists()
    assert scheduler.cancel_reserved_upload(reservation.job_id) is False
    assert scheduler.cancel_reserved_upload(uuid4()) is False


async def test_telegram_going_away_pauses_admission_and_the_pump(
    scheduler: JobScheduler, bus: EventEmitter, recorder: Recorder
) -> None:
    bus.emit(
        TELEGRAM_UNAVAILABLE,
        TelegramUnavailable(ErrorInfo("telegram_unavailable", "Gone"), _stamp()),
    )

    assert scheduler.accepting is False
    with pytest.raises(SchedulerError) as error:
        scheduler.submit_video(VIDEO)
    assert error.value.code == "scheduler_unavailable"
    await settle()
    assert recorder.facts == []


async def test_stopping_releases_reservations_and_queued_uploads(
    scheduler: JobScheduler, storage: ArtifactStorage
) -> None:
    queued = await stage_upload(scheduler, storage)
    scheduler.enqueue_reserved_upload(queued, 3)
    reserved = await stage_upload(scheduler, storage)

    scheduler.stop()

    assert scheduler.accepting is False
    assert not (storage.root / str(queued.job_id)).exists()
    assert not (storage.root / str(reserved.job_id)).exists()


async def test_a_handler_failure_makes_the_scheduler_stop_accepting(
    scheduler: JobScheduler, bus: EventEmitter
) -> None:
    def explode(_event: object) -> None:
        raise RuntimeError("listener down")

    bus.on(JOB_QUEUED, explode)

    with pytest.raises(RuntimeError):
        scheduler.submit_video(VIDEO)
    assert scheduler.accepting is False


async def test_failing_the_scheduler_drops_the_active_staged_upload(
    scheduler: JobScheduler, storage: ArtifactStorage
) -> None:
    reservation = await stage_upload(scheduler, storage)
    scheduler.enqueue_reserved_upload(reservation, 3)
    await settle()

    scheduler.fail()

    assert scheduler.accepting is False
    assert not (storage.root / str(reservation.job_id)).exists()


async def test_the_error_topic_is_never_published_by_the_scheduler(
    scheduler: JobScheduler, bus: EventEmitter
) -> None:
    seen: list[object] = []
    bus.on(ERROR, seen.append)
    scheduler.submit_video(VIDEO)
    await settle()
    assert seen == []
