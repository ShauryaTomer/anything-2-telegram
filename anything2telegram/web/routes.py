"""HTML routes for the operator page. Registered onto the jobs API app."""

from pathlib import Path

from urllib.parse import parse_qs, urlsplit
from uuid import UUID

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from anything2telegram.domain import BatchEntry, JobSnapshot, JobStatus, telegram_message_url
from anything2telegram.downloaders.youtube import (
    UnsupportedYouTubeUrl,
    YouTubeUrlKind,
    classify_youtube_url,
)
from anything2telegram.jobs.scheduler import SchedulerError
from anything2telegram.tui import format_bytes

_WEB_ROOT = Path(__file__).parent
_TEMPLATES = Jinja2Templates(directory=_WEB_ROOT / "templates")
_TEMPLATES.env.filters["format_bytes"] = format_bytes
_TEMPLATES.env.filters["video_id"] = lambda url: _query_or_tail(url, "v")
_TEMPLATES.env.filters["playlist_id"] = lambda url: _query_or_tail(url, "list")
_NOT_READY = "Not ready — the Telegram client is still connecting. Try again in a moment."


def _query_or_tail(url: str, param: str) -> str:
    """A muted fallback id: the URL's own query param, or its last path segment."""
    values = parse_qs(urlsplit(url).query).get(param)
    return values[0] if values else url.rsplit("/", 1)[-1]


def _parse_open(raw: str) -> tuple[frozenset[str], str]:
    """The open set as ids (for membership checks) and its canonical query value."""
    ids = frozenset(part for part in raw.split(",") if part)
    return ids, ",".join(sorted(ids))


def _toggle_open(open_ids: frozenset[str], batch_id: object) -> str:
    """The open set with batch_id's membership flipped, as a query value."""
    next_ids = set(open_ids)
    next_ids.symmetric_difference_update({str(batch_id)})
    return ",".join(sorted(next_ids))


_TEMPLATES.env.globals["toggle_open"] = _toggle_open


def _ui_offset(raw: object) -> int | None:
    """Parse the 1-indexed offset form field; None means invalid, not absent."""
    if raw is None:
        return 1
    if not isinstance(raw, str) or not raw.strip():
        return None
    try:
        value = int(raw)
    except ValueError:
        return None
    return value if value >= 1 else None


def _queue_entries(request: Request) -> tuple[object, ...]:
    tracker = getattr(request.app.state, "tracker", None)
    if tracker is None:
        return ()
    return tracker.list_queue()


def _row_context(request: Request, open: str = "") -> dict[str, object]:
    """Live progress and completed-upload links, keyed by job id.

    The template has no app.state access, so every registry/settings read
    happens here and gets handed over as plain per-job mappings. `open` is
    the raw ?open= query value, parsed once here for all four routes that
    render queue.html so their notion of the open set never drifts apart.
    """
    open_ids, open_param = _parse_open(open)
    entries = _queue_entries(request)
    registry = getattr(request.app.state, "progress", None)
    settings = getattr(request.app.state, "settings", None)
    topic_id = getattr(settings, "topic_id", None)
    progress: dict[UUID, object] = {}
    links: dict[UUID, str] = {}

    def collect(job: JobSnapshot) -> None:
        if registry is not None:
            current = registry.get(job.id)
            if current is not None:
                progress[job.id] = current
        if (
            job.status is JobStatus.COMPLETED
            and job.telegram_chat_id is not None
            and job.telegram_message_id is not None
        ):
            links[job.id] = telegram_message_url(
                job.telegram_chat_id, job.telegram_message_id, topic_id
            )

    for entry in entries:
        if isinstance(entry, BatchEntry):
            for job in entry.jobs:
                collect(job)
        else:
            collect(entry)

    return {
        "entries": entries,
        "progress": progress,
        "links": links,
        "open": open_ids,
        "open_param": open_param,
    }


def _submit_response(
    request: Request, flash_code: str | None, flash_message: str | None, open: str = ""
) -> HTMLResponse:
    return _TEMPLATES.TemplateResponse(
        request,
        "submit_response.html",
        _row_context(request, open)
        | {"flash_code": flash_code, "flash_message": flash_message},
    )


def register_web_routes(app: FastAPI) -> None:
    # Deferred import: anything2telegram.api.jobs imports register_web_routes
    # from this module, so importing it back at module scope would cycle.
    from anything2telegram.api.jobs import (
        _declared_size,
        _is_ready,
        _stage_upload_and_enqueue,
        _UploadSubmitError,
    )

    app.mount(
        "/web/static",
        StaticFiles(directory=_WEB_ROOT / "static"),
        name="web-static",
    )

    @app.get("/", response_class=HTMLResponse)
    async def page(request: Request, open: str = Query("")) -> HTMLResponse:
        return _TEMPLATES.TemplateResponse(request, "page.html", _row_context(request, open))

    @app.get("/web/queue", response_class=HTMLResponse)
    async def queue_fragment(request: Request, open: str = Query("")) -> HTMLResponse:
        return _TEMPLATES.TemplateResponse(request, "queue.html", _row_context(request, open))

    @app.post("/web/youtube", response_class=HTMLResponse)
    async def submit_youtube(request: Request, open: str = Query("")) -> HTMLResponse:
        # Parsed from the raw form (not a typed FastAPI Form(...) param): a
        # binding failure there raises RequestValidationError before this body
        # runs, which the app-wide handler turns into a JSON 422 — breaking
        # this route's "always 200" contract.
        form = await request.form()
        url = form.get("url")
        if not isinstance(url, str) or not url:
            return _submit_response(request, "422", "Not a supported YouTube URL.", open)

        offset = _ui_offset(form.get("offset"))
        if offset is None:
            return _submit_response(request, "422", "Request is invalid.", open)

        try:
            kind = classify_youtube_url(url)
        except UnsupportedYouTubeUrl:
            return _submit_response(request, "422", "Not a supported YouTube URL.", open)

        if not _is_ready(request):
            return _submit_response(request, "503", _NOT_READY, open)

        scheduler = request.app.state.scheduler
        try:
            if kind is YouTubeUrlKind.VIDEO:
                scheduler.submit_video(url)
            else:
                scheduler.submit_playlist(url, offset - 1)
        except SchedulerError:
            return _submit_response(request, "503", _NOT_READY, open)

        return _submit_response(request, None, None, open)

    @app.post("/web/upload", response_class=HTMLResponse)
    async def submit_upload(request: Request, open: str = Query("")) -> HTMLResponse:
        if not _is_ready(request):
            return _submit_response(request, "503", _NOT_READY, open)

        state = request.app.state
        max_bytes = state.settings.max_artifact_bytes
        if _declared_size(request) > max_bytes:
            return _submit_response(
                request, "413", "The file is larger than the staging limit.", open
            )

        try:
            await _stage_upload_and_enqueue(request, state, max_bytes)
        except _UploadSubmitError as error:
            return _submit_response(request, str(error.status_code), error.message, open)

        return _submit_response(request, None, None, open)
