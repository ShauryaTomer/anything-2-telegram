from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import Mock
from uuid import UUID, uuid4

import pytest
from pyee import EventEmitter
from pyee.asyncio import AsyncIOEventEmitter

from anything2telegram.domain import ErrorInfo
from anything2telegram.events import (
    ARTIFACT_PRODUCTION_FAILED,
    ARTIFACT_UPLOADED,
    ARTIFACT_UPLOAD_FAILED,
    ArtifactProductionFailed,
    ArtifactUploaded,
    ArtifactUploadFailed,
)


NOW = datetime(2026, 7, 19, 8, 30, tzinfo=UTC)


def _cleanup_api():
    from anything2telegram.artifacts.cleanup import ArtifactCleanup

    return ArtifactCleanup


class RecordingStorage:
    def __init__(self, error: Exception | None = None) -> None:
        self.deleted: list[UUID] = []
        self.error = error

    def delete_job_directory(self, job_id: UUID) -> None:
        self.deleted.append(job_id)
        if self.error is not None:
            raise self.error


def _events(job_id: UUID):
    artifact_id = uuid4()
    error = ErrorInfo("failed", "Operation failed")
    return [
        (
            ARTIFACT_UPLOADED,
            ArtifactUploaded(job_id, artifact_id, -1001, 7, NOW),
        ),
        (
            ARTIFACT_UPLOAD_FAILED,
            ArtifactUploadFailed(job_id, artifact_id, error, NOW),
        ),
        (
            ARTIFACT_PRODUCTION_FAILED,
            ArtifactProductionFailed(job_id, artifact_id, error, NOW),
        ),
    ]


@pytest.mark.parametrize("bus_type", [EventEmitter, AsyncIOEventEmitter])
def test_register_deletes_correct_job_for_all_terminal_artifact_topics(
    bus_type: type[EventEmitter],
) -> None:
    ArtifactCleanup = _cleanup_api()
    storage = RecordingStorage()
    logger = Mock()
    bus = bus_type()
    ArtifactCleanup(storage, logger).register(bus)
    job_ids = [uuid4(), uuid4(), uuid4()]

    for job_id, (topic, event) in zip(job_ids, _events(job_ids[0])):
        event = type(event)(job_id=job_id, **{
            field: getattr(event, field)
            for field in event.__dataclass_fields__
            if field != "job_id"
        })
        assert bus.emit(topic, event) is True

    assert storage.deleted == job_ids
    logger.error.assert_not_called()


def test_cleanup_failure_is_logged_safely_and_swallowed() -> None:
    ArtifactCleanup = _cleanup_api()
    storage = RecordingStorage(RuntimeError("/secret/artifact/path"))
    logger = Mock()
    bus = EventEmitter()
    ArtifactCleanup(storage, logger).register(bus)
    topic, event = _events(uuid4())[0]
    original = event

    assert bus.emit(topic, event) is True

    assert event == original
    logger.error.assert_called_once()
    args, kwargs = logger.error.call_args
    rendered = " ".join(str(value) for value in (*args, *kwargs.values()))
    assert "/secret" not in rendered
    assert str(event.job_id) not in rendered


def test_cleanup_logger_failure_is_swallowed_and_later_listeners_run() -> None:
    ArtifactCleanup = _cleanup_api()
    storage = RecordingStorage(RuntimeError("delete failed"))
    logger = Mock()
    logger.error.side_effect = RuntimeError("logger failed")
    bus = EventEmitter()
    ArtifactCleanup(storage, logger).register(bus)
    topic, event = _events(uuid4())[0]
    later_events: list[object] = []
    bus.on(topic, later_events.append)

    assert bus.emit(topic, event) is True

    assert later_events == [event]
