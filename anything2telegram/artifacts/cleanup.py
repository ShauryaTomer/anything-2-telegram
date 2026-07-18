from typing import Protocol

from ..events import (
    ARTIFACT_PRODUCTION_FAILED,
    ARTIFACT_UPLOADED,
    ARTIFACT_UPLOAD_FAILED,
)


class EventBus(Protocol):
    def on(self, event: str, handler: object) -> object: ...


class ArtifactCleanup:
    def __init__(self, storage: object, logger: object) -> None:
        self._storage = storage
        self._logger = logger

    def register(self, bus: EventBus) -> None:
        bus.on(ARTIFACT_UPLOADED, self._delete_job)
        bus.on(ARTIFACT_UPLOAD_FAILED, self._delete_job)
        bus.on(ARTIFACT_PRODUCTION_FAILED, self._delete_job)

    def _delete_job(self, event: object) -> None:
        try:
            self._storage.delete_job_directory(event.job_id)
        except Exception:
            self._logger.error("Artifact cleanup failed")
