"""Pure Job lifecycle state machine.

Every question of the form "may this fact be applied to a Job whose
persisted status is X?" is answered here and only here. The tracker is a
persistence adapter around this module: it loads the row, calls apply(),
and writes the row back when apply() says it changed.
"""

from ..domain import JobPhase, JobStatus
from ..events import (
    ArtifactProductionFailed,
    ArtifactReady,
    ArtifactUploaded,
    ArtifactUploadFailed,
    JobStarted,
)
from .repositories import JobRow


JobFact = (
    JobStarted
    | ArtifactReady
    | ArtifactProductionFailed
    | ArtifactUploaded
    | ArtifactUploadFailed
)

_TERMINAL = (JobStatus.COMPLETED, JobStatus.FAILED)


class TrackingError(RuntimeError):
    """Lifecycle fact cannot be applied to the persisted projection."""


class InvalidJobTransition(TrackingError):
    """Job lifecycle fact conflicts with its current state."""


# The transition table: statuses each fact may be applied from.
# INTERRUPTED admits JOB_STARTED because startup recovery re-queues the same
# row and the pump re-emits JOB_STARTED for it. ARTIFACT_READY admits
# UPLOADING only for staged jobs (the scheduler emits READY itself for
# staged uploads, after JOB_STARTED already moved the row to UPLOADING).
_ALLOWED_FROM: dict[type, tuple[JobStatus, ...]] = {
    JobStarted: (JobStatus.WAITING, JobStatus.INTERRUPTED),
    ArtifactReady: (JobStatus.PRODUCING, JobStatus.UPLOADING),
    ArtifactProductionFailed: (JobStatus.PRODUCING,),
    ArtifactUploaded: (JobStatus.UPLOADING,),
    ArtifactUploadFailed: (JobStatus.UPLOADING,),
}

# JobPhase values must remain a subset of JobStatus values; this mapping is
# that invariant's one enforced home.
_STATUS_FOR_PHASE = {
    JobPhase.PRODUCING: JobStatus.PRODUCING,
    JobPhase.UPLOADING: JobStatus.UPLOADING,
}


def apply(record: JobRow, fact: JobFact) -> bool:
    """Apply one lifecycle fact to a job row, mutating it in place.

    Returns True when the row changed (caller must persist it), False when
    the fact is an exact duplicate of one already applied (no-op). Raises
    TrackingError/InvalidJobTransition when the fact conflicts with the
    row's current state.
    """
    if _is_duplicate(record, fact):
        return False
    if record.status not in _ALLOWED_FROM[type(fact)]:
        raise InvalidJobTransition("invalid job transition")
    _APPLY[type(fact)](record, fact)
    record.updated_at = fact.occurred_at
    return True


def _is_duplicate(record: JobRow, fact: JobFact) -> bool:
    """Detect redelivery of an already-applied fact; raise on conflicts."""
    if isinstance(fact, ArtifactReady):
        if record.ready_local_path is None:
            return False
        if _matches_ready_event(record, fact):
            return True
        raise InvalidJobTransition("conflicting artifact ready event")
    if isinstance(fact, JobStarted):
        return False
    if record.status not in _TERMINAL:
        return False
    if _matches_terminal_event(record, fact):
        return True
    raise InvalidJobTransition("conflicting job terminal event")


def _apply_started(record: JobRow, fact: JobStarted) -> None:
    expected_phase = JobPhase.UPLOADING if record.staged else JobPhase.PRODUCING
    if fact.phase is not expected_phase:
        raise InvalidJobTransition("invalid job phase")
    record.status = _STATUS_FOR_PHASE[fact.phase]


def _apply_ready(record: JobRow, fact: ArtifactReady) -> None:
    if record.staged:
        _require_artifact(record, fact.artifact_id)
    elif record.status is JobStatus.UPLOADING:
        raise InvalidJobTransition("invalid job transition")
    record.status = JobStatus.UPLOADING
    record.artifact_id = fact.artifact_id
    record.filename = fact.filename
    record.size_bytes = fact.size_bytes
    record.ready_local_path = fact.local_path
    record.ready_media_type = fact.media_type
    record.ready_caption = fact.caption


def _apply_production_failed(record: JobRow, fact: ArtifactProductionFailed) -> None:
    if fact.artifact_id is not None:
        if record.artifact_id is not None:
            _require_artifact(record, fact.artifact_id)
        record.artifact_id = fact.artifact_id
    record.status = JobStatus.FAILED
    record.error = fact.error


def _apply_uploaded(record: JobRow, fact: ArtifactUploaded) -> None:
    _require_artifact(record, fact.artifact_id)
    record.status = JobStatus.COMPLETED
    record.telegram_chat_id = fact.telegram_chat_id
    record.telegram_message_id = fact.telegram_message_id


def _apply_upload_failed(record: JobRow, fact: ArtifactUploadFailed) -> None:
    _require_artifact(record, fact.artifact_id)
    record.status = JobStatus.FAILED
    record.error = fact.error


_APPLY = {
    JobStarted: _apply_started,
    ArtifactReady: _apply_ready,
    ArtifactProductionFailed: _apply_production_failed,
    ArtifactUploaded: _apply_uploaded,
    ArtifactUploadFailed: _apply_upload_failed,
}


def _require_artifact(record: JobRow, artifact_id) -> None:
    if record.artifact_id != artifact_id:
        raise TrackingError("artifact does not match job")


def _matches_ready_event(record: JobRow, event: ArtifactReady) -> bool:
    return (
        record.artifact_id == event.artifact_id
        and record.ready_local_path == event.local_path
        and record.filename == event.filename
        and record.ready_media_type == event.media_type
        and record.size_bytes == event.size_bytes
        and record.ready_caption == event.caption
        and record.updated_at == event.occurred_at
    )


def _matches_terminal_event(
    record: JobRow,
    event: ArtifactProductionFailed | ArtifactUploaded | ArtifactUploadFailed,
) -> bool:
    if record.updated_at != event.occurred_at:
        return False
    if isinstance(event, ArtifactUploaded):
        return (
            record.status is JobStatus.COMPLETED
            and record.artifact_id == event.artifact_id
            and record.telegram_chat_id == event.telegram_chat_id
            and record.telegram_message_id == event.telegram_message_id
        )
    if record.status is not JobStatus.FAILED or record.error != event.error:
        return False
    if isinstance(event, ArtifactProductionFailed):
        return event.artifact_id is None or record.artifact_id == event.artifact_id
    return record.artifact_id == event.artifact_id
