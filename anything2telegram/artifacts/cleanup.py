import logging

from ..events import (
    ARTIFACT_PRODUCTION_FAILED,
    ARTIFACT_UPLOADED,
    ARTIFACT_UPLOAD_FAILED,
)


_LOGGER = logging.getLogger(__name__)


class ArtifactCleanup:
    """Removes a job's staged files once the job can no longer need them."""

    def __init__(self, storage) -> None:
        self._storage = storage

    def register(self, bus) -> None:
        bus.on(ARTIFACT_UPLOADED, self._delete_job)
        bus.on(ARTIFACT_UPLOAD_FAILED, self._delete_job)
        bus.on(ARTIFACT_PRODUCTION_FAILED, self._delete_job)

    def _delete_job(self, event) -> None:
        try:
            self._storage.delete_job_directory(event.job_id)
        except Exception:
            _LOGGER.exception("Artifact cleanup failed")
