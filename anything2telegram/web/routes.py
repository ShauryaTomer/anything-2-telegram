"""HTML routes for the operator page. Registered onto the jobs API app."""

from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

_WEB_ROOT = Path(__file__).parent
_TEMPLATES = Jinja2Templates(directory=_WEB_ROOT / "templates")


def _queue_entries(request: Request) -> tuple[object, ...]:
    tracker = getattr(request.app.state, "tracker", None)
    if tracker is None:
        return ()
    return tracker.list_queue()


def register_web_routes(app: FastAPI) -> None:
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
