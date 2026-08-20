"""Pure state-machine tests: build a row at any status directly, apply one
fact, assert — no bus, no repositories, no event choreography."""

from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

from anything2telegram.domain import ErrorInfo, JobPhase, JobStatus, SourceKind
from anything2telegram.events import (
    ArtifactProductionFailed,
    ArtifactReady,
    ArtifactUploaded,
    ArtifactUploadFailed,
    JobStarted,
)
from anything2telegram.jobs import lifecycle
from anything2telegram.jobs.lifecycle import InvalidJobTransition, TrackingError
from anything2telegram.jobs.repositories import JobRow


JOB_1 = UUID(int=1)
ARTIFACT_1 = UUID(int=11)
ARTIFACT_2 = UUID(int=12)
ERROR = ErrorInfo(code="boom", message="Boom")


def time(step: int) -> datetime:
    return datetime(2026, 1, 1, 0, 0, step, tzinfo=UTC)


def make_row(
    status: JobStatus,
    *,
    staged: bool = False,
    artifact_id: UUID | None = None,
    **overrides,
) -> JobRow:
    row = JobRow(
        id=JOB_1,
        batch_id=None,
        source_kind=SourceKind.LOCAL_UPLOAD if staged else SourceKind.YOUTUBE,
        source="https://example.com/v",
        title=None,
        status=status,
        artifact_id=artifact_id,
        filename=None,
        size_bytes=None,
        telegram_chat_id=None,
        telegram_message_id=None,
        error=None,
        created_at=time(0),
        updated_at=time(0),
        staged=staged,
    )
    for name, value in overrides.items():
        setattr(row, name, value)
    return row


def started(phase: JobPhase = JobPhase.PRODUCING) -> JobStarted:
    return JobStarted(JOB_1, phase, time(1))


def ready(artifact_id: UUID = ARTIFACT_1) -> ArtifactReady:
    return ArtifactReady(
        job_id=JOB_1,
        artifact_id=artifact_id,
        local_path=Path("/artifacts/a.mp4"),
        filename="a.mp4",
        media_type="video/mp4",
        size_bytes=42,
        caption="A",
        occurred_at=time(2),
    )


def uploaded(artifact_id: UUID = ARTIFACT_1) -> ArtifactUploaded:
    return ArtifactUploaded(JOB_1, artifact_id, 100, 200, time(3))


def production_failed(artifact_id: UUID | None = None) -> ArtifactProductionFailed:
    return ArtifactProductionFailed(JOB_1, artifact_id, ERROR, time(3))


def upload_failed(artifact_id: UUID = ARTIFACT_1) -> ArtifactUploadFailed:
    return ArtifactUploadFailed(JOB_1, artifact_id, ERROR, time(3))


def fact_for(fact_type: type, row: JobRow):
    if fact_type is JobStarted:
        return started(JobPhase.UPLOADING if row.staged else JobPhase.PRODUCING)
    if fact_type is ArtifactReady:
        return ready()
    if fact_type is ArtifactProductionFailed:
        return production_failed()
    if fact_type is ArtifactUploaded:
        return uploaded()
    return upload_failed()


ALLOWED = {
    JobStarted: {JobStatus.WAITING, JobStatus.INTERRUPTED},
    ArtifactReady: {JobStatus.PRODUCING, JobStatus.UPLOADING},
    ArtifactProductionFailed: {JobStatus.PRODUCING},
    ArtifactUploaded: {JobStatus.UPLOADING},
    ArtifactUploadFailed: {JobStatus.UPLOADING},
}


@pytest.mark.parametrize("fact_type", list(ALLOWED))
@pytest.mark.parametrize("status", list(JobStatus))
def test_transition_table(fact_type: type, status: JobStatus) -> None:
    """Every (fact, prior status) pair either applies or is rejected —
    exhaustively, straight off the table."""
    staged = fact_type is ArtifactReady and status is JobStatus.UPLOADING
    row = make_row(status, staged=staged, artifact_id=ARTIFACT_1)
    fact = fact_for(fact_type, row)
    if status in ALLOWED[fact_type]:
        assert lifecycle.apply(row, fact) is True
        assert row.updated_at == fact.occurred_at
    else:
        with pytest.raises(TrackingError):
            lifecycle.apply(row, fact)


@pytest.mark.parametrize(
    ("staged", "phase", "expected"),
    [
        (False, JobPhase.PRODUCING, JobStatus.PRODUCING),
        (True, JobPhase.UPLOADING, JobStatus.UPLOADING),
    ],
)
def test_started_sets_status_for_phase(staged, phase, expected) -> None:
    row = make_row(JobStatus.WAITING, staged=staged, artifact_id=ARTIFACT_1)
    assert lifecycle.apply(row, started(phase)) is True
    assert row.status is expected


def test_production_start_clears_artifact_state_from_an_earlier_attempt() -> None:
    row = make_row(
        JobStatus.WAITING,
        artifact_id=ARTIFACT_1,
        filename="old.mp4",
        size_bytes=42,
        telegram_chat_id=100,
        telegram_message_id=200,
        error=ERROR,
        ready_local_path=Path("/artifacts/old.mp4"),
        ready_media_type="video/mp4",
        ready_caption="old",
    )

    lifecycle.apply(row, started())

    assert row.artifact_id is None
    assert row.filename is None
    assert row.size_bytes is None
    assert row.telegram_chat_id is None
    assert row.telegram_message_id is None
    assert row.error is None
    assert row.ready_local_path is None
    assert row.ready_media_type is None
    assert row.ready_caption is None


@pytest.mark.parametrize("prior", [JobStatus.WAITING, JobStatus.INTERRUPTED])
def test_started_rejects_wrong_phase(prior: JobStatus) -> None:
    row = make_row(prior)
    with pytest.raises(InvalidJobTransition):
        lifecycle.apply(row, started(JobPhase.UPLOADING))


def test_ready_records_artifact_fields() -> None:
    row = make_row(JobStatus.PRODUCING)
    event = ready()
    assert lifecycle.apply(row, event) is True
    assert row.status is JobStatus.UPLOADING
    assert row.artifact_id == event.artifact_id
    assert row.ready_local_path == event.local_path
    assert row.ready_media_type == event.media_type
    assert row.ready_caption == event.caption


def test_ready_duplicate_is_noop() -> None:
    row = make_row(JobStatus.PRODUCING)
    event = ready()
    lifecycle.apply(row, event)
    assert lifecycle.apply(row, event) is False


def test_ready_conflicting_redelivery_rejected() -> None:
    row = make_row(JobStatus.PRODUCING)
    lifecycle.apply(row, ready())
    with pytest.raises(InvalidJobTransition):
        lifecycle.apply(row, ready(ARTIFACT_2))


def test_ready_rejects_unstaged_uploading_job() -> None:
    row = make_row(JobStatus.UPLOADING, artifact_id=ARTIFACT_1)
    with pytest.raises(InvalidJobTransition):
        lifecycle.apply(row, ready())


def test_ready_rejects_staged_artifact_mismatch() -> None:
    row = make_row(JobStatus.UPLOADING, staged=True, artifact_id=ARTIFACT_1)
    with pytest.raises(TrackingError):
        lifecycle.apply(row, ready(ARTIFACT_2))


def test_uploaded_completes_job() -> None:
    row = make_row(JobStatus.UPLOADING, artifact_id=ARTIFACT_1)
    assert lifecycle.apply(row, uploaded()) is True
    assert row.status is JobStatus.COMPLETED
    assert row.telegram_chat_id == 100
    assert row.telegram_message_id == 200


@pytest.mark.parametrize("fact", [uploaded(ARTIFACT_2), upload_failed(ARTIFACT_2)])
def test_terminal_facts_reject_artifact_mismatch(fact) -> None:
    row = make_row(JobStatus.UPLOADING, artifact_id=ARTIFACT_1)
    with pytest.raises(TrackingError):
        lifecycle.apply(row, fact)


def test_terminal_duplicate_is_noop() -> None:
    row = make_row(JobStatus.UPLOADING, artifact_id=ARTIFACT_1)
    event = uploaded()
    lifecycle.apply(row, event)
    assert lifecycle.apply(row, event) is False


def test_conflicting_terminal_fact_rejected() -> None:
    row = make_row(JobStatus.UPLOADING, artifact_id=ARTIFACT_1)
    lifecycle.apply(row, uploaded())
    with pytest.raises(InvalidJobTransition):
        lifecycle.apply(row, upload_failed())


def test_production_failed_adopts_artifact_id() -> None:
    row = make_row(JobStatus.PRODUCING)
    assert lifecycle.apply(row, production_failed(ARTIFACT_1)) is True
    assert row.status is JobStatus.FAILED
    assert row.artifact_id == ARTIFACT_1
    assert row.error == ERROR


def test_production_failed_rejects_artifact_mismatch() -> None:
    row = make_row(JobStatus.PRODUCING, artifact_id=ARTIFACT_1)
    with pytest.raises(TrackingError):
        lifecycle.apply(row, production_failed(ARTIFACT_2))
