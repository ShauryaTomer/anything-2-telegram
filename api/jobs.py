from datetime import datetime
from typing import Protocol
from uuid import UUID

from fastapi import FastAPI, File, Form, UploadFile
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.requests import ClientDisconnect

from anything2telegram.artifacts.storage import ArtifactStorageError
from anything2telegram.domain import (
    BatchSnapshot,
    ErrorInfo,
    JobSnapshot,
    StagedArtifact,
    UploadReservation,
)
from anything2telegram.downloaders.youtube import (
    UnsupportedYouTubeUrl,
    YouTubeUrlKind,
    classify_youtube_url,
)
from anything2telegram.jobs.scheduler import SchedulerError


class Scheduler(Protocol):
    def submit_youtube_video(self, source_url: str) -> UUID: ...

    def submit_playlist_expansion(self, source_url: str) -> UUID: ...

    def reserve_local_upload(
        self,
        filename: str,
        media_type: str | None,
        caption: str | None,
    ) -> UploadReservation: ...

    def enqueue_reserved_upload(self, staged: StagedArtifact) -> UUID: ...

    def cancel_reserved_upload(self, job_id: UUID) -> bool: ...


class Tracker(Protocol):
    def get_job(self, job_id: UUID) -> JobSnapshot | None: ...

    def get_batch(self, batch_id: UUID) -> BatchSnapshot | None: ...


class Storage(Protocol):
    async def stage(
        self,
        upload: UploadFile,
        reservation: UploadReservation,
        max_bytes: int,
    ) -> StagedArtifact: ...


class Readiness(Protocol):
    def is_accepting(self) -> bool: ...


class TelegramConnectivity(Protocol):
    def is_connected(self) -> bool: ...


class _YouTubeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: str


def _error(status_code: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"detail": {"code": code, "message": message}},
    )


def _service_unavailable() -> JSONResponse:
    return _error(503, "service_unavailable", "Service is not ready")


def _safe_bool(check: object, method_name: str) -> bool:
    try:
        method = getattr(check, method_name)
        return method() is True
    except Exception:
        return False


def _timestamp(value: datetime) -> str:
    rendered = value.isoformat()
    if rendered.endswith("+00:00"):
        return f"{rendered[:-6]}Z"
    return rendered


def _error_info(value: ErrorInfo | None) -> dict[str, str] | None:
    if value is None:
        return None
    return {"code": value.code, "message": value.message}


def _job_body(snapshot: JobSnapshot) -> dict[str, object]:
    return {
        "id": str(snapshot.id),
        "batch_id": str(snapshot.batch_id) if snapshot.batch_id else None,
        "source_kind": snapshot.source_kind.value,
        "source": snapshot.source,
        "status": snapshot.status.value,
        "artifact_id": str(snapshot.artifact_id) if snapshot.artifact_id else None,
        "filename": snapshot.filename,
        "size_bytes": snapshot.size_bytes,
        "telegram_message_id": snapshot.telegram_message_id,
        "error": _error_info(snapshot.error),
        "created_at": _timestamp(snapshot.created_at),
        "updated_at": _timestamp(snapshot.updated_at),
    }


def _batch_body(snapshot: BatchSnapshot) -> dict[str, object]:
    return {
        "id": str(snapshot.id),
        "source_url": snapshot.source_url,
        "status": snapshot.status.value,
        "total_jobs": snapshot.total_jobs,
        "waiting": snapshot.waiting,
        "producing": snapshot.producing,
        "uploading": snapshot.uploading,
        "completed": snapshot.completed,
        "failed": snapshot.failed,
        "skipped_entries": snapshot.skipped_entries,
        "job_ids": [str(job_id) for job_id in snapshot.job_ids],
        "error": _error_info(snapshot.error),
        "created_at": _timestamp(snapshot.created_at),
        "updated_at": _timestamp(snapshot.updated_at),
    }


def _submission_body(kind: str, identifier: UUID, path: str) -> dict[str, str]:
    return {
        "type": kind,
        "id": str(identifier),
        "status_url": f"/{path}/{identifier}",
    }


def _cancel_reservation(scheduler: Scheduler, job_id: UUID) -> None:
    try:
        scheduler.cancel_reserved_upload(job_id)
    except Exception:
        pass


def _storage_error(error: ArtifactStorageError) -> JSONResponse:
    if error.code == "staging_oversize":
        return _error(
            413,
            "staging_oversize",
            "Upload exceeds maximum allowed size",
        )
    if error.code == "staging_disk_full":
        return _error(507, "staging_disk_full", "Artifact storage is full")
    return _error(
        500,
        "upload_staging_failed",
        "Upload could not be stored",
    )


def create_jobs_app(
    *,
    scheduler: Scheduler,
    tracker: Tracker,
    storage: Storage,
    readiness: Readiness,
    telegram: TelegramConnectivity,
    max_upload_bytes: int,
) -> FastAPI:
    if type(max_upload_bytes) is not int or max_upload_bytes < 0:
        raise ValueError("max_upload_bytes must be a nonnegative int")

    app = FastAPI()

    def accepting() -> bool:
        return _safe_bool(readiness, "is_accepting") and _safe_bool(
            telegram, "is_connected"
        )

    @app.exception_handler(RequestValidationError)
    async def invalid_request(
        _request: object, _error_value: RequestValidationError
    ) -> JSONResponse:
        return _error(422, "invalid_request", "Request is invalid")

    @app.exception_handler(StarletteHTTPException)
    async def framework_http_error(
        _request: object, error: StarletteHTTPException
    ) -> JSONResponse:
        if error.status_code == 404:
            return _error(404, "not_found", "Resource not found")
        if error.status_code == 405:
            return _error(405, "method_not_allowed", "Method not allowed")
        if 400 <= error.status_code < 500:
            return _error(
                error.status_code,
                "invalid_request",
                "Request is invalid",
            )
        return _error(500, "internal_error", "Internal server error")

    @app.post("/jobs/youtube", status_code=202)
    async def submit_youtube(request: _YouTubeRequest) -> object:
        try:
            kind = classify_youtube_url(request.url)
        except UnsupportedYouTubeUrl:
            return _error(
                422,
                "unsupported_youtube_url",
                "YouTube URL is unsupported",
            )
        if not accepting():
            return _service_unavailable()
        try:
            if kind is YouTubeUrlKind.VIDEO:
                job_id = scheduler.submit_youtube_video(request.url)
                return _submission_body("job", job_id, "jobs")
            batch_id = scheduler.submit_playlist_expansion(request.url)
            return _submission_body("batch", batch_id, "batches")
        except SchedulerError as error:
            if error.code == "scheduler_stopped":
                return _service_unavailable()
            return _error(500, "submission_failed", "Submission failed")
        except Exception:
            return _error(500, "submission_failed", "Submission failed")

    @app.post("/jobs/upload", status_code=202)
    async def submit_upload(
        file: UploadFile = File(...), caption: str | None = Form(None)
    ) -> object:
        if not accepting():
            return _service_unavailable()
        if not file.filename:
            return _error(422, "invalid_request", "Request is invalid")
        try:
            reservation = scheduler.reserve_local_upload(
                file.filename,
                file.content_type,
                caption,
            )
        except SchedulerError as error:
            if error.code == "scheduler_stopped":
                return _service_unavailable()
            return _error(
                500,
                "upload_reservation_failed",
                "Upload could not be reserved",
            )
        except ArtifactStorageError:
            return _error(422, "invalid_request", "Request is invalid")
        except (TypeError, ValueError):
            return _error(422, "invalid_request", "Request is invalid")
        except Exception:
            return _error(
                500,
                "upload_reservation_failed",
                "Upload could not be reserved",
            )

        try:
            staged = await storage.stage(file, reservation, max_upload_bytes)
            job_id = scheduler.enqueue_reserved_upload(staged)
        except ArtifactStorageError as error:
            _cancel_reservation(scheduler, reservation.job_id)
            return _storage_error(error)
        except SchedulerError as error:
            _cancel_reservation(scheduler, reservation.job_id)
            if error.code == "scheduler_stopped":
                return _service_unavailable()
            return _error(
                500,
                "upload_enqueue_failed",
                "Upload could not be queued",
            )
        except ClientDisconnect:
            _cancel_reservation(scheduler, reservation.job_id)
            raise
        except Exception:
            _cancel_reservation(scheduler, reservation.job_id)
            return _error(
                500,
                "upload_staging_failed",
                "Upload could not be stored",
            )
        except BaseException:
            _cancel_reservation(scheduler, reservation.job_id)
            raise
        return _submission_body("job", job_id, "jobs")

    @app.get("/jobs/{job_id}")
    async def get_job(job_id: str) -> object:
        try:
            identifier = UUID(job_id)
        except (TypeError, ValueError):
            return _error(404, "job_not_found", "Job not found")
        snapshot = tracker.get_job(identifier)
        if snapshot is None:
            return _error(404, "job_not_found", "Job not found")
        return _job_body(snapshot)

    @app.get("/batches/{batch_id}")
    async def get_batch(batch_id: str) -> object:
        try:
            identifier = UUID(batch_id)
        except (TypeError, ValueError):
            return _error(404, "batch_not_found", "Batch not found")
        snapshot = tracker.get_batch(identifier)
        if snapshot is None:
            return _error(404, "batch_not_found", "Batch not found")
        return _batch_body(snapshot)

    @app.get("/health")
    async def health() -> JSONResponse:
        telegram_connected = _safe_bool(telegram, "is_connected")
        ready = _safe_bool(readiness, "is_accepting") and telegram_connected
        return JSONResponse(
            status_code=200 if ready else 503,
            content={
                "ready": ready,
                "telegram_connected": telegram_connected,
            },
        )

    return app
