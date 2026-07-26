"""HTTP job API. Components are read from ``app.state`` per request."""

import errno
import logging
from datetime import datetime
from uuid import UUID

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field
from python_multipart.exceptions import MultipartParseError
from starlette.datastructures import UploadFile
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.formparsers import MultiPartException
from starlette.requests import ClientDisconnect

from anything2telegram.artifacts.storage import ArtifactStorageError
from anything2telegram.domain import (
    BatchRef,
    BatchSnapshot,
    ErrorInfo,
    JobRef,
    JobSnapshot,
)
from anything2telegram.downloaders.youtube import (
    UnsupportedYouTubeUrl,
    YouTubeUrlKind,
    classify_youtube_url,
)
from anything2telegram.jobs.scheduler import SchedulerError


_LOGGER = logging.getLogger(__name__)
_UPLOAD_FIELDS = {"file", "caption"}


class _YouTubeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: str
    offset: int = Field(default=0, ge=0)


def _response(
    status_code: int,
    code: str,
    message: str,
    *,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"detail": {"code": code, "message": message}},
        headers=headers,
    )


def _service_unavailable() -> JSONResponse:
    return _response(503, "service_unavailable", "Service is not ready")


def _invalid_request() -> JSONResponse:
    return _response(422, "invalid_request", "Request is invalid")


def _oversize() -> JSONResponse:
    return _response(413, "staging_oversize", "Upload exceeds maximum allowed size")


def _storage_error(error: ArtifactStorageError) -> JSONResponse:
    _LOGGER.warning("Upload staging failed: code=%s", error.code)
    if error.code == "invalid_filename":
        return _response(422, "invalid_filename", "Artifact filename is invalid")
    if error.code == "staging_oversize":
        return _oversize()
    if error.code == "staging_disk_full":
        return _response(507, "staging_disk_full", "Artifact storage is full")
    return _response(500, "upload_staging_failed", "Upload could not be stored")


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


def _submission_body(kind: str, ref: JobRef | BatchRef) -> dict[str, str]:
    return {"type": kind, "id": str(ref.id), "status_url": ref.status_url}


def _identifier(value: str) -> UUID | None:
    try:
        return UUID(value)
    except ValueError:
        return None


class _DisconnectAwareUpload:
    """Stops feeding the staging writer as soon as the client goes away."""

    def __init__(self, request: Request, upload: UploadFile) -> None:
        self._request = request
        self._upload = upload

    async def read(self, size: int) -> bytes:
        if await self._request.is_disconnected():
            raise ClientDisconnect()
        return await self._upload.read(size)


def _is_ready(request: Request) -> bool:
    state = request.app.state
    if getattr(state, "readiness", None) is None:
        return False
    return (
        state.readiness.is_accepting()
        and state.telegram.is_connected
        and state.scheduler.accepting
    )


def _declared_size(request: Request) -> int:
    declared = request.headers.get("content-length", "")
    return int(declared) if declared.isdigit() else 0


def create_jobs_app() -> FastAPI:
    app = FastAPI()

    @app.exception_handler(RequestValidationError)
    async def invalid_body(_request: object, _exception: object) -> JSONResponse:
        return _invalid_request()

    @app.exception_handler(StarletteHTTPException)
    async def framework_http_error(
        _request: object, exception: StarletteHTTPException
    ) -> JSONResponse:
        status_code = exception.status_code
        if status_code == 404:
            return _response(
                404, "not_found", "Resource not found", headers=exception.headers
            )
        if status_code == 405:
            return _response(
                405,
                "method_not_allowed",
                "Method not allowed",
                headers=exception.headers,
            )
        if 400 <= status_code < 500:
            return _response(
                status_code,
                "invalid_request",
                "Request is invalid",
                headers=exception.headers,
            )
        return _response(500, "internal_error", "Internal server error")

    @app.exception_handler(Exception)
    async def internal_error(_request: object, exception: Exception) -> JSONResponse:
        if isinstance(exception, ClientDisconnect):
            raise exception
        _LOGGER.exception("Unhandled API request failure")
        return _response(500, "internal_error", "Internal server error")

    @app.post("/jobs/youtube", status_code=202)
    async def submit_youtube(request: Request, body: _YouTubeRequest) -> object:
        try:
            kind = classify_youtube_url(body.url)
        except UnsupportedYouTubeUrl:
            return _response(
                422, "unsupported_youtube_url", "YouTube URL is unsupported"
            )
        if not _is_ready(request):
            return _service_unavailable()
        scheduler = request.app.state.scheduler
        try:
            if kind is YouTubeUrlKind.VIDEO:
                return _submission_body("job", scheduler.submit_video(body.url))
            return _submission_body(
                "batch", scheduler.submit_playlist(body.url, body.offset)
            )
        except SchedulerError:
            return _service_unavailable()

    @app.post("/jobs/upload", status_code=202)
    async def submit_upload(request: Request) -> object:
        if not _is_ready(request):
            return _service_unavailable()
        state = request.app.state
        max_bytes = state.settings.max_artifact_bytes
        if _declared_size(request) > max_bytes:
            return _oversize()

        try:
            form = await request.form(max_files=1, max_fields=1)
        except OSError as error:
            _LOGGER.exception("Multipart body could not be buffered")
            if error.errno == errno.ENOSPC:
                return _response(507, "staging_disk_full", "Artifact storage is full")
            return _response(500, "upload_staging_failed", "Upload could not be stored")
        except (MultiPartException, MultipartParseError):
            return _invalid_request()

        try:
            file = form.get("file")
            caption = form.get("caption")
            if (
                not isinstance(file, UploadFile)
                or not file.filename
                or not isinstance(caption, (str, type(None)))
                or set(form.keys()) - _UPLOAD_FIELDS
            ):
                return _invalid_request()

            try:
                reservation = state.scheduler.reserve_local_upload(
                    file.filename, file.content_type, caption
                )
            except SchedulerError:
                return _service_unavailable()
            except ArtifactStorageError as error:
                return _storage_error(error)

            try:
                staged = await state.storage.stage(
                    _DisconnectAwareUpload(request, file), reservation, max_bytes
                )
                return _submission_body(
                    "job",
                    state.scheduler.enqueue_reserved_upload(
                        reservation, staged.size_bytes
                    ),
                )
            except ArtifactStorageError as error:
                return _storage_error(error)
            except SchedulerError:
                state.scheduler.cancel_reserved_upload(reservation.job_id)
                return _service_unavailable()
            except BaseException:
                state.scheduler.cancel_reserved_upload(reservation.job_id)
                raise
        finally:
            await form.close()

    @app.get("/jobs/{job_id}")
    async def get_job(request: Request, job_id: str) -> object:
        identifier = _identifier(job_id)
        snapshot = (
            None
            if identifier is None
            else request.app.state.tracker.get_job(identifier)
        )
        if snapshot is None:
            return _response(404, "job_not_found", "Job not found")
        return _job_body(snapshot)

    @app.get("/batches/{batch_id}")
    async def get_batch(request: Request, batch_id: str) -> object:
        identifier = _identifier(batch_id)
        snapshot = (
            None
            if identifier is None
            else request.app.state.tracker.get_batch(identifier)
        )
        if snapshot is None:
            return _response(404, "batch_not_found", "Batch not found")
        return _batch_body(snapshot)

    @app.get("/health")
    async def health(request: Request) -> JSONResponse:
        telegram = getattr(request.app.state, "telegram", None)
        connected = telegram is not None and telegram.is_connected
        ready = _is_ready(request)
        return JSONResponse(
            status_code=200 if ready else 503,
            content={"ready": ready, "telegram_connected": connected},
        )

    return app
