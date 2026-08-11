"""HTML routes for the operator page. Registered onto the jobs API app."""

from pathlib import Path

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from anything2telegram.downloaders.youtube import (
    UnsupportedYouTubeUrl,
    YouTubeUrlKind,
    classify_youtube_url,
)
from anything2telegram.jobs.scheduler import SchedulerError

_WEB_ROOT = Path(__file__).parent
_TEMPLATES = Jinja2Templates(directory=_WEB_ROOT / "templates")
_NOT_READY = "Not ready — the Telegram client is still connecting. Try again in a moment."


def _queue_entries(request: Request) -> tuple[object, ...]:
    tracker = getattr(request.app.state, "tracker", None)
    if tracker is None:
        return ()
    return tracker.list_queue()


def _submit_response(
    request: Request, flash_code: str | None, flash_message: str | None
) -> HTMLResponse:
    return _TEMPLATES.TemplateResponse(
        request,
        "submit_response.html",
        {
            "entries": _queue_entries(request),
            "flash_code": flash_code,
            "flash_message": flash_message,
        },
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
    async def page(request: Request) -> HTMLResponse:
        return _TEMPLATES.TemplateResponse(
            request, "page.html", {"entries": _queue_entries(request)}
        )

    @app.get("/web/queue", response_class=HTMLResponse)
    async def queue_fragment(request: Request) -> HTMLResponse:
        return _TEMPLATES.TemplateResponse(
            request, "queue.html", {"entries": _queue_entries(request)}
        )

    @app.post("/web/youtube", response_class=HTMLResponse)
    async def submit_youtube(
        request: Request,
        url: str = Form(...),
        offset: int = Form(default=1, ge=1),
    ) -> HTMLResponse:
        try:
            kind = classify_youtube_url(url)
        except UnsupportedYouTubeUrl:
            return _submit_response(request, "422", "Not a supported YouTube URL.")

        if not _is_ready(request):
            return _submit_response(request, "503", _NOT_READY)

        scheduler = request.app.state.scheduler
        try:
            if kind is YouTubeUrlKind.VIDEO:
                scheduler.submit_video(url)
            else:
                scheduler.submit_playlist(url, offset - 1)
        except SchedulerError:
            return _submit_response(request, "503", _NOT_READY)

        return _submit_response(request, None, None)

    @app.post("/web/upload", response_class=HTMLResponse)
    async def submit_upload(request: Request) -> HTMLResponse:
        if not _is_ready(request):
            return _submit_response(request, "503", _NOT_READY)

        state = request.app.state
        max_bytes = state.settings.max_artifact_bytes
        if _declared_size(request) > max_bytes:
            return _submit_response(
                request, "413", "The file is larger than the staging limit."
            )

        try:
            await _stage_upload_and_enqueue(request, state, max_bytes)
        except _UploadSubmitError as error:
            return _submit_response(request, str(error.status_code), error.message)

        return _submit_response(request, None, None)
