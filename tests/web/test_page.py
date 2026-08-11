from datetime import UTC, datetime
from uuid import UUID

from httpx import ASGITransport, AsyncClient
from pyee.asyncio import AsyncIOEventEmitter

from anything2telegram.api.jobs import create_jobs_app
from anything2telegram.domain import SourceKind
from anything2telegram.events import (
    BATCH_CREATED,
    BATCH_JOBS_CREATED,
    JOB_QUEUED,
    BatchCreated,
    BatchJobsCreated,
    JobQueued,
)
from anything2telegram.jobs.tracker import JobTracker

AT = datetime(2026, 7, 26, 12, 0, tzinfo=UTC)
JOB = UUID("10000000-0000-0000-0000-000000000001")
BATCH = UUID("20000000-0000-0000-0000-000000000001")
CHILD_1 = UUID("10000000-0000-0000-0000-000000000002")
CHILD_2 = UUID("10000000-0000-0000-0000-000000000003")


async def _client(tracker: JobTracker | None = None) -> AsyncClient:
    app = create_jobs_app()
    if tracker is not None:
        app.state.tracker = tracker
    transport = ASGITransport(app=app)
    return AsyncClient(transport=transport, base_url="http://service")


def _tracker_with_a_standalone_job_and_a_batch() -> JobTracker:
    bus = AsyncIOEventEmitter()
    tracker = JobTracker()
    tracker.register(bus)

    bus.emit(
        JOB_QUEUED,
        JobQueued(JOB, None, SourceKind.YOUTUBE, "https://youtu.be/standalone", None, AT),
    )
    bus.emit(BATCH_CREATED, BatchCreated(BATCH, "https://youtu.be/playlist?list=x", AT))
    for child_id in (CHILD_1, CHILD_2):
        bus.emit(
            JOB_QUEUED,
            JobQueued(
                child_id, BATCH, SourceKind.YOUTUBE, f"https://youtu.be/{child_id}", None, AT
            ),
        )
    bus.emit(BATCH_JOBS_CREATED, BatchJobsCreated(BATCH, (CHILD_1, CHILD_2), 0, AT))
    return tracker


async def test_page_renders_headers_empty_state_and_flash_placeholder() -> None:
    async with await _client() as client:
        response = await client.get("/")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    body = response.text
    assert '<div id="flash"></div>' in body
    assert 'id="queue"' in body
    assert 'hx-get="/web/queue"' in body
    assert 'hx-trigger="every 2s"' in body
    assert 'hx-swap="outerHTML"' in body
    assert "<th>Source</th>" in body
    assert "<th>Status</th>" in body
    assert "<th>Progress</th>" in body
    assert "<th>Link</th>" in body
    assert "Nothing queued. Paste a YouTube URL or pick a file above." in body
    assert '<meta name="color-scheme" content="light dark">' in body


async def test_page_has_two_forms_with_expected_fields() -> None:
    async with await _client() as client:
        response = await client.get("/")

    body = response.text
    assert body.count("<form") == 2
    assert 'name="url"' in body
    assert 'name="offset"' in body
    assert 'name="file"' in body


async def test_vendored_htmx_is_served_locally() -> None:
    async with await _client() as client:
        response = await client.get("/web/static/vendor/htmx-2.0.9.min.js")

    assert response.status_code == 200
    assert "javascript" in response.headers["content-type"]


async def test_vendored_pico_is_served_locally() -> None:
    async with await _client() as client:
        response = await client.get("/web/static/vendor/pico-2.1.1.classless.min.css")

    assert response.status_code == 200
    assert "css" in response.headers["content-type"]


async def test_existing_json_routes_still_resolve() -> None:
    async with await _client() as client:
        health = await client.get("/health")
        openapi = await client.get("/openapi.json")
        docs = await client.get("/docs")
        missing_job = await client.get("/jobs/not-a-uuid")

    assert health.status_code == 503
    assert health.json() == {"ready": False, "telegram_connected": False}
    assert openapi.status_code == 200
    assert docs.status_code == 200
    assert missing_job.status_code == 404
    assert missing_job.json()["detail"]["code"] == "job_not_found"


async def test_web_queue_fragment_always_answers_200_with_its_own_poll_attrs() -> None:
    async with await _client() as client:
        response = await client.get("/web/queue")

    assert response.status_code == 200
    body = response.text
    assert 'id="queue"' in body
    assert 'hx-get="/web/queue"' in body
    assert 'hx-trigger="every 2s"' in body
    assert 'hx-swap="outerHTML"' in body
    assert "Nothing queued. Paste a YouTube URL or pick a file above." in body


async def test_batch_children_are_numbered_zero_padded_to_the_batchs_width() -> None:
    bus = AsyncIOEventEmitter()
    tracker = JobTracker()
    tracker.register(bus)
    children = tuple(UUID(f"30000000-0000-0000-0000-{index:012d}") for index in range(1, 11))

    bus.emit(BATCH_CREATED, BatchCreated(BATCH, "https://youtu.be/playlist?list=big", AT))
    for child_id in children:
        bus.emit(
            JOB_QUEUED,
            JobQueued(child_id, BATCH, SourceKind.YOUTUBE, str(child_id), None, AT),
        )
    bus.emit(BATCH_JOBS_CREATED, BatchJobsCreated(BATCH, children, 0, AT))

    async with await _client(tracker) as client:
        response = await client.get("/web/queue")

    body = response.text
    assert f"03/10 {children[2]}" in body
    assert f"10/10 {children[9]}" in body


async def test_page_and_fragment_render_the_same_live_queue_rows() -> None:
    tracker = _tracker_with_a_standalone_job_and_a_batch()

    async with await _client(tracker) as client:
        page = await client.get("/")
        fragment = await client.get("/web/queue")

    assert page.status_code == fragment.status_code == 200
    for body in (page.text, fragment.text):
        assert "https://youtu.be/standalone" in body
        assert "https://youtu.be/playlist?list=x" in body
        assert "waiting" in body
        assert body.count('class="queue-child"') == 2
        assert f"1/2 https://youtu.be/{CHILD_1}" in body
        assert f"2/2 https://youtu.be/{CHILD_2}" in body
