from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pyee import EventEmitter

from anything2telegram.artifacts.cleanup import ArtifactCleanup
from anything2telegram.domain import ErrorInfo
from anything2telegram.events import (
    ARTIFACT_PRODUCTION_FAILED,
    ARTIFACT_UPLOAD_FAILED,
    ARTIFACT_UPLOADED,
    ArtifactProductionFailed,
    ArtifactUploaded,
    ArtifactUploadFailed,
)


class RecordingStorage:
    def __init__(self, *, failing: bool = False) -> None:
        self.deleted: list[object] = []
        self._failing = failing

    def delete_job_directory(self, job_id: object) -> None:
        if self._failing:
            raise OSError("busy")
        self.deleted.append(job_id)


def _now() -> datetime:
    return datetime.now(UTC)


@pytest.mark.parametrize(
    "topic,event_for",
    [
        (
            ARTIFACT_UPLOADED,
            lambda job_id: ArtifactUploaded(job_id, uuid4(), -1001, 7, _now()),
        ),
        (
            ARTIFACT_UPLOAD_FAILED,
            lambda job_id: ArtifactUploadFailed(
                job_id, uuid4(), ErrorInfo("boom", "Boom"), _now()
            ),
        ),
        (
            ARTIFACT_PRODUCTION_FAILED,
            lambda job_id: ArtifactProductionFailed(
                job_id, None, ErrorInfo("boom", "Boom"), _now()
            ),
        ),
    ],
)
def test_every_terminal_topic_deletes_that_job(topic: str, event_for) -> None:
    storage = RecordingStorage()
    bus = EventEmitter()
    ArtifactCleanup(storage).register(bus)
    job_id = uuid4()

    bus.emit(topic, event_for(job_id))

    assert storage.deleted == [job_id]


def test_a_failing_delete_is_logged_and_later_listeners_still_run() -> None:
    bus = EventEmitter()
    ArtifactCleanup(RecordingStorage(failing=True)).register(bus)
    seen: list[object] = []
    bus.on(ARTIFACT_UPLOADED, seen.append)

    bus.emit(ARTIFACT_UPLOADED, ArtifactUploaded(uuid4(), uuid4(), -1001, 7, _now()))

    assert len(seen) == 1
