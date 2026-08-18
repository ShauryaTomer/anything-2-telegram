from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

from httpx import ASGITransport, AsyncClient
from pyee.asyncio import AsyncIOEventEmitter

from anything2telegram.api.jobs import create_jobs_app
from anything2telegram.domain import JobPhase, SourceKind, StagedArtifact
from anything2telegram.events import (
    ARTIFACT_READY,
    ARTIFACT_UPLOADED,
    BATCH_CREATED,
    BATCH_JOBS_CREATED,
    JOB_QUEUED,
    JOB_STARTED,
    YOUTUBE_PLAYLIST_EXPANDED,
    ArtifactReady,
    ArtifactUploaded,
    BatchCreated,
    BatchJobsCreated,
    JobQueued,
    JobStarted,
    PlaylistExpanded,
)
from anything2telegram.jobs.progress import Progress
from anything2telegram.jobs.repositories import BatchesRepository, JobsRepository, open_database
from anything2telegram.jobs.tracker import JobTracker

AT = datetime(2026, 7, 26, 12, 0, tzinfo=UTC)
JOB = UUID("10000000-0000-0000-0000-000000000001")
BATCH = UUID("20000000-0000-0000-0000-000000000001")
CHILD_1 = UUID("10000000-0000-0000-0000-000000000002")
CHILD_2 = UUID("10000000-0000-0000-0000-000000000003")


class FakeProgressRegistry:
    def __init__(self) -> None:
        self.values: dict[UUID, Progress] = {}

    def get(self, job_id: UUID) -> Progress | None:
        return self.values.get(job_id)


async def _build_tracker(tmp_path: Path) -> tuple[JobTracker, object]:
    conn = await open_database(tmp_path / "yt2tg.sqlite3")
    return JobTracker(JobsRepository(conn), BatchesRepository(conn)), conn


async def _drain(bus: AsyncIOEventEmitter) -> None:
    """Async handlers run as scheduled tasks, not inline; let them finish."""
    while not bus.complete:
        await bus.wait_for_complete()


async def _emit(bus: AsyncIOEventEmitter, topic: str, event: object) -> None:
    """Emit, then drain, since handlers are causally dependent across calls."""
    bus.emit(topic, event)
    await _drain(bus)


async def _client(
    tracker: JobTracker | None = None,
    *,
    progress: FakeProgressRegistry | None = None,
    topic_id: int | None = None,
) -> AsyncClient:
    app = create_jobs_app()
    if tracker is not None:
        app.state.tracker = tracker
    if progress is not None:
        app.state.progress = progress
    if topic_id is not None:
        app.state.settings = SimpleNamespace(topic_id=topic_id)
    transport = ASGITransport(app=app)
    return AsyncClient(transport=transport, base_url="http://service")


async def _tracker_with_a_standalone_job_and_a_batch(tmp_path: Path) -> JobTracker:
    bus = AsyncIOEventEmitter()
    tracker, _conn = await _build_tracker(tmp_path)
    tracker.register(bus)

    await _emit(bus, 
        JOB_QUEUED,
        JobQueued(JOB, None, SourceKind.YOUTUBE, "https://youtu.be/standalone", None, AT),
    )
    await _emit(bus, BATCH_CREATED, BatchCreated(BATCH, "https://youtu.be/playlist?list=x", AT))
    for child_id in (CHILD_1, CHILD_2):
        await _emit(bus, 
            JOB_QUEUED,
            JobQueued(
                child_id, BATCH, SourceKind.YOUTUBE, f"https://youtu.be/{child_id}", None, AT
            ),
        )
    await _emit(bus, BATCH_JOBS_CREATED, BatchJobsCreated(BATCH, (CHILD_1, CHILD_2), 0, AT))
    await _drain(bus)
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


async def test_page_indents_batch_children_with_css() -> None:
    async with await _client() as client:
        response = await client.get("/")

    body = response.text
    assert ".queue-child" in body
    assert "padding-left" in body


async def test_web_queue_orders_interleaved_jobs_and_batches_newest_first(tmp_path: Path) -> None:
    bus = AsyncIOEventEmitter()
    tracker, _conn = await _build_tracker(tmp_path)
    tracker.register(bus)
    job_a = UUID("10000000-0000-0000-0000-0000000000a1")
    batch_a = UUID("20000000-0000-0000-0000-0000000000a1")
    job_b = UUID("10000000-0000-0000-0000-0000000000b1")
    batch_b = UUID("20000000-0000-0000-0000-0000000000b1")

    await _emit(bus, 
        JOB_QUEUED,
        JobQueued(job_a, None, SourceKind.YOUTUBE, "https://youtu.be/job-a", None, AT),
    )
    await _emit(bus, 
        BATCH_CREATED,
        BatchCreated(
            batch_a, "https://youtu.be/playlist?list=a", AT + timedelta(seconds=1)
        ),
    )
    await _emit(bus, 
        JOB_QUEUED,
        JobQueued(
            job_b,
            None,
            SourceKind.YOUTUBE,
            "https://youtu.be/job-b",
            None,
            AT + timedelta(seconds=2),
        ),
    )
    await _emit(bus, 
        BATCH_CREATED,
        BatchCreated(
            batch_b, "https://youtu.be/playlist?list=b", AT + timedelta(seconds=3)
        ),
    )

    await _drain(bus)

    async with await _client(tracker) as client:
        page = await client.get("/")
        fragment = await client.get("/web/queue")

    for body in (page.text, fragment.text):
        positions = [
            body.index(marker)
            for marker in (
                "https://youtu.be/playlist?list=b",
                "https://youtu.be/job-b",
                "https://youtu.be/playlist?list=a",
                "https://youtu.be/job-a",
            )
        ]
        assert positions == sorted(positions)


async def test_web_queue_renders_an_expanding_batch_with_no_children(tmp_path: Path) -> None:
    bus = AsyncIOEventEmitter()
    tracker, _conn = await _build_tracker(tmp_path)
    tracker.register(bus)
    batch_id = UUID("20000000-0000-0000-0000-000000000099")
    await _emit(bus, 
        BATCH_CREATED,
        BatchCreated(batch_id, "https://youtu.be/playlist?list=expanding", AT),
    )

    await _drain(bus)

    async with await _client(tracker) as client:
        page = await client.get("/")
        fragment = await client.get("/web/queue")

    assert page.status_code == fragment.status_code == 200
    for body in (page.text, fragment.text):
        assert "https://youtu.be/playlist?list=expanding" in body
        assert "expanding" in body
        assert "waiting" in body
        assert 'class="queue-child"' not in body


async def test_batch_children_are_numbered_zero_padded_to_the_batchs_width(tmp_path: Path) -> None:
    bus = AsyncIOEventEmitter()
    tracker, _conn = await _build_tracker(tmp_path)
    tracker.register(bus)
    children = tuple(UUID(f"30000000-0000-0000-0000-{index:012d}") for index in range(1, 11))

    await _emit(bus, BATCH_CREATED, BatchCreated(BATCH, "https://youtu.be/playlist?list=big", AT))
    for child_id in children:
        await _emit(bus, 
            JOB_QUEUED,
            JobQueued(child_id, BATCH, SourceKind.YOUTUBE, str(child_id), None, AT),
        )
    await _emit(bus, BATCH_JOBS_CREATED, BatchJobsCreated(BATCH, children, 0, AT))

    await _drain(bus)

    async with await _client(tracker) as client:
        response = await client.get(f"/web/queue?open={BATCH}")

    body = response.text
    assert f'03/10 <span class="muted">{children[2]}</span>' in body
    assert f'10/10 <span class="muted">{children[9]}</span>' in body


async def test_page_and_fragment_render_the_same_live_queue_rows(tmp_path: Path) -> None:
    tracker = await _tracker_with_a_standalone_job_and_a_batch(tmp_path)

    async with await _client(tracker) as client:
        page = await client.get(f"/?open={BATCH}")
        fragment = await client.get(f"/web/queue?open={BATCH}")

    assert page.status_code == fragment.status_code == 200
    for body in (page.text, fragment.text):
        assert "https://youtu.be/standalone" in body
        assert "https://youtu.be/playlist?list=x" in body
        assert "waiting" in body
        assert body.count('class="queue-child"') == 2
        # Titles are unknown, so the Source cell falls back to the bare video
        # id; the full URL still appears once, behind the row's copy button.
        assert f'1/2 <span class="muted">{CHILD_1}</span>' in body
        assert f'2/2 <span class="muted">{CHILD_2}</span>' in body


async def test_progress_cell_renders_indeterminate_bytes_determinate_percentage_or_blank(tmp_path: Path) -> None:
    bus = AsyncIOEventEmitter()
    tracker, _conn = await _build_tracker(tmp_path)
    tracker.register(bus)
    producing_id, uploading_id, waiting_id = uuid4(), uuid4(), uuid4()

    await _emit(bus, 
        JOB_QUEUED,
        JobQueued(producing_id, None, SourceKind.YOUTUBE, "https://youtu.be/aaaaaaaaaaa", None, AT),
    )
    await _emit(bus, JOB_STARTED, JobStarted(producing_id, JobPhase.PRODUCING, AT))

    await _emit(bus, 
        JOB_QUEUED,
        JobQueued(uploading_id, None, SourceKind.YOUTUBE, "https://youtu.be/bbbbbbbbbbb", None, AT),
    )
    await _emit(bus, JOB_STARTED, JobStarted(uploading_id, JobPhase.PRODUCING, AT))
    await _emit(bus, 
        ARTIFACT_READY,
        ArtifactReady(
            uploading_id, uuid4(), Path("/tmp/clip.mp4"), "clip.mp4", "video/mp4", 200, None, AT
        ),
    )

    await _emit(bus, 
        JOB_QUEUED,
        JobQueued(waiting_id, None, SourceKind.YOUTUBE, "https://youtu.be/ccccccccccc", None, AT),
    )

    progress = FakeProgressRegistry()
    progress.values[producing_id] = Progress(JobPhase.PRODUCING, 2_500_000, 0)
    progress.values[uploading_id] = Progress(JobPhase.UPLOADING, 50, 200)

    await _drain(bus)

    async with await _client(tracker, progress=progress) as client:
        response = await client.get("/web/queue")

    body = response.text
    assert "<progress></progress>" in body
    assert "2.5 MB" in body
    assert '<progress value="50" max="200">' in body
    assert "25%" in body
    # The waiting job has no live transfer, so its Progress cell stays blank.
    assert body.count("<progress") == 2


async def test_link_cell_is_empty_until_completed_then_carries_the_derived_url_and_never_for_batches(tmp_path: Path) -> None:
    bus = AsyncIOEventEmitter()
    tracker, _conn = await _build_tracker(tmp_path)
    tracker.register(bus)
    completed_id, waiting_id, artifact_id = uuid4(), uuid4(), uuid4()

    await _emit(bus, 
        JOB_QUEUED,
        JobQueued(completed_id, None, SourceKind.YOUTUBE, "https://youtu.be/aaaaaaaaaaa", None, AT),
    )
    await _emit(bus, JOB_STARTED, JobStarted(completed_id, JobPhase.PRODUCING, AT))
    await _emit(bus, 
        ARTIFACT_READY,
        ArtifactReady(
            completed_id, artifact_id, Path("/tmp/clip.mp4"), "clip.mp4", "video/mp4", 200, None, AT
        ),
    )
    await _emit(bus, 
        ARTIFACT_UPLOADED,
        ArtifactUploaded(completed_id, artifact_id, -1001234567890, 55, AT),
    )

    await _emit(bus, 
        JOB_QUEUED,
        JobQueued(waiting_id, None, SourceKind.YOUTUBE, "https://youtu.be/bbbbbbbbbbb", None, AT),
    )
    await _emit(bus, BATCH_CREATED, BatchCreated(BATCH, "https://youtu.be/playlist?list=x", AT))

    await _drain(bus)

    async with await _client(tracker, topic_id=7) as client:
        response = await client.get("/web/queue")

    body = response.text
    assert '<a href="https://t.me/c/1234567890/7/55">open</a>' in body
    # Neither the waiting job nor the (unrelated, expanding) batch link out.
    assert body.count("<a href=") == 1


async def test_source_cell_shows_names_and_falls_back_to_muted_identifiers(tmp_path: Path) -> None:
    bus = AsyncIOEventEmitter()
    tracker, _conn = await _build_tracker(tmp_path)
    tracker.register(bus)

    await _emit(bus, 
        BATCH_CREATED,
        BatchCreated(BATCH, "https://www.youtube.com/playlist?list=PL123", AT),
    )
    await _emit(bus, 
        JOB_QUEUED,
        JobQueued(
            CHILD_1,
            BATCH,
            SourceKind.YOUTUBE,
            "https://www.youtube.com/watch?v=aaaaaaaaaaa",
            None,
            AT,
            title="Episode One",
        ),
    )
    await _emit(bus, 
        JOB_QUEUED,
        JobQueued(
            CHILD_2, BATCH, SourceKind.YOUTUBE, "https://www.youtube.com/watch?v=bbbbbbbbbbb", None, AT
        ),
    )
    await _emit(bus, BATCH_JOBS_CREATED, BatchJobsCreated(BATCH, (CHILD_1, CHILD_2), 0, AT))
    await _emit(bus, 
        YOUTUBE_PLAYLIST_EXPANDED,
        PlaylistExpanded(BATCH, (), 0, AT, "Rust Fundamentals"),
    )

    batch_2 = uuid4()
    await _emit(bus, 
        BATCH_CREATED,
        BatchCreated(batch_2, "https://www.youtube.com/playlist?list=PLNOEXPAND", AT),
    )

    pre_filename_id = uuid4()
    await _emit(bus, 
        JOB_QUEUED,
        JobQueued(
            pre_filename_id, None, SourceKind.YOUTUBE, "https://www.youtube.com/watch?v=ccccccccccc", None, AT
        ),
    )

    post_filename_id = uuid4()
    await _emit(bus, 
        JOB_QUEUED,
        JobQueued(
            post_filename_id, None, SourceKind.YOUTUBE, "https://www.youtube.com/watch?v=ddddddddddd", None, AT
        ),
    )
    await _emit(bus, JOB_STARTED, JobStarted(post_filename_id, JobPhase.PRODUCING, AT))
    await _emit(bus, 
        ARTIFACT_READY,
        ArtifactReady(
            post_filename_id, uuid4(), Path("/tmp/known.mp4"), "known.mp4", "video/mp4", 5, None, AT
        ),
    )

    staged = StagedArtifact(uuid4(), uuid4(), Path("/tmp/upload.mp4"), "upload.mp4", "video/mp4", 5, None)
    await _emit(bus, 
        JOB_QUEUED,
        JobQueued(staged.job_id, None, SourceKind.LOCAL_UPLOAD, staged.filename, staged, AT),
    )

    await _drain(bus)

    async with await _client(tracker) as client:
        response = await client.get(f"/web/queue?open={BATCH}")

    body = response.text
    assert "Rust Fundamentals" in body
    assert "Episode One" in body
    assert '<span class="muted">bbbbbbbbbbb</span>' in body
    assert '<span class="muted">PLNOEXPAND</span>' in body
    assert '<span class="muted">ccccccccccc</span>' in body
    assert "known.mp4" in body
    assert "upload.mp4" in body


async def test_copy_button_appears_only_for_rows_with_an_original_url(tmp_path: Path) -> None:
    bus = AsyncIOEventEmitter()
    tracker, _conn = await _build_tracker(tmp_path)
    tracker.register(bus)

    await _emit(bus, 
        JOB_QUEUED,
        JobQueued(uuid4(), None, SourceKind.YOUTUBE, "https://youtu.be/aaaaaaaaaaa", None, AT),
    )
    staged = StagedArtifact(uuid4(), uuid4(), Path("/tmp/upload.mp4"), "upload.mp4", "video/mp4", 5, None)
    await _emit(bus, 
        JOB_QUEUED,
        JobQueued(staged.job_id, None, SourceKind.LOCAL_UPLOAD, staged.filename, staged, AT),
    )

    await _drain(bus)

    async with await _client(tracker) as client:
        response = await client.get("/web/queue")

    body = response.text
    assert body.count('class="copy"') == 1


async def test_copy_button_url_with_an_apostrophe_is_escaped_not_injected(tmp_path: Path) -> None:
    """Regression: the copy button must never interpolate the URL into a JS
    string literal, where an escaped apostrophe (decoded by the browser
    before the JS parser runs) could break out and execute arbitrary code."""
    bus = AsyncIOEventEmitter()
    tracker, _conn = await _build_tracker(tmp_path)
    tracker.register(bus)
    malicious = "https://www.youtube.com/watch?v=aaaaaaaaaaa&z='-alert(1)-'"

    await _emit(bus, 
        JOB_QUEUED,
        JobQueued(uuid4(), None, SourceKind.YOUTUBE, malicious, None, AT),
    )

    await _drain(bus)

    async with await _client(tracker) as client:
        response = await client.get("/web/queue")

    body = response.text
    assert "onclick" not in body
    assert (
        'data-url="https://www.youtube.com/watch?v=aaaaaaaaaaa&amp;z=&#39;-alert(1)-&#39;"'
        in body
    )


async def test_failed_job_status_cell_carries_title_with_error_code_and_message(tmp_path: Path) -> None:
    from anything2telegram.domain import ErrorInfo
    from anything2telegram.events import ARTIFACT_PRODUCTION_FAILED, ArtifactProductionFailed

    bus = AsyncIOEventEmitter()
    tracker, _conn = await _build_tracker(tmp_path)
    tracker.register(bus)
    job_id = uuid4()

    await _emit(bus, 
        JOB_QUEUED,
        JobQueued(job_id, None, SourceKind.YOUTUBE, "https://youtu.be/aaaaaaaaaaa", None, AT),
    )
    await _emit(bus, JOB_STARTED, JobStarted(job_id, JobPhase.PRODUCING, AT))
    error = ErrorInfo("youtube_timeout", "YouTube operation timed out")
    await _emit(bus, 
        ARTIFACT_PRODUCTION_FAILED,
        ArtifactProductionFailed(job_id, None, error, AT),
    )

    await _drain(bus)

    async with await _client(tracker) as client:
        response = await client.get("/web/queue")

    body = response.text
    assert '<span class="failed" title="youtube_timeout — YouTube operation timed out">failed</span>' in body


async def test_failed_batch_status_cell_carries_title_with_error_code_and_message(tmp_path: Path) -> None:
    from anything2telegram.domain import ErrorInfo
    from anything2telegram.events import YOUTUBE_PLAYLIST_EXPANSION_FAILED, PlaylistExpansionFailed

    bus = AsyncIOEventEmitter()
    tracker, _conn = await _build_tracker(tmp_path)
    tracker.register(bus)
    batch_id = uuid4()

    await _emit(bus, 
        BATCH_CREATED,
        BatchCreated(batch_id, "https://www.youtube.com/playlist?list=PLexpand", AT),
    )
    error = ErrorInfo("youtube_playlist_expansion_failed", "Playlist expansion failed")
    await _emit(bus, 
        YOUTUBE_PLAYLIST_EXPANSION_FAILED,
        PlaylistExpansionFailed(batch_id, error, AT),
    )

    await _drain(bus)

    async with await _client(tracker) as client:
        response = await client.get("/web/queue")

    body = response.text
    assert '<span class="failed" title="youtube_playlist_expansion_failed — Playlist expansion failed">failed</span>' in body


async def test_aggregate_count_on_partially_completed_batch_carries_no_title(tmp_path: Path) -> None:
    bus = AsyncIOEventEmitter()
    tracker, _conn = await _build_tracker(tmp_path)
    tracker.register(bus)
    batch_id = uuid4()
    child_1 = uuid4()
    child_2 = uuid4()

    await _emit(bus, 
        BATCH_CREATED,
        BatchCreated(batch_id, "https://www.youtube.com/playlist?list=PLtest", AT),
    )
    await _emit(bus, 
        JOB_QUEUED,
        JobQueued(child_1, batch_id, SourceKind.YOUTUBE, "https://youtu.be/child1", None, AT),
    )
    await _emit(bus, 
        JOB_QUEUED,
        JobQueued(child_2, batch_id, SourceKind.YOUTUBE, "https://youtu.be/child2", None, AT),
    )
    await _emit(bus, BATCH_JOBS_CREATED, BatchJobsCreated(batch_id, (child_1, child_2), 0, AT))

    # Emit a failure for child_1
    from anything2telegram.domain import ErrorInfo
    from anything2telegram.events import ARTIFACT_PRODUCTION_FAILED, ArtifactProductionFailed

    await _emit(bus, JOB_STARTED, JobStarted(child_1, JobPhase.PRODUCING, AT))
    error = ErrorInfo("child_failed", "Child job failed")
    await _emit(bus, 
        ARTIFACT_PRODUCTION_FAILED,
        ArtifactProductionFailed(child_1, None, error, AT),
    )

    # Drive child_2 to COMPLETED to reach PARTIALLY_COMPLETED batch status (one completed + one failed)
    artifact_id_2 = uuid4()
    await _emit(bus, JOB_STARTED, JobStarted(child_2, JobPhase.PRODUCING, AT))
    await _emit(bus, 
        ARTIFACT_READY,
        ArtifactReady(
            child_2, artifact_id_2, Path("/tmp/clip.mp4"), "clip.mp4", "video/mp4", 200, None, AT
        ),
    )
    await _emit(bus, 
        ARTIFACT_UPLOADED,
        ArtifactUploaded(child_2, artifact_id_2, -1001234567890, 55, AT),
    )

    await _drain(bus)

    async with await _client(tracker) as client:
        response = await client.get(f"/web/queue?open={batch_id}")

    body = response.text
    # The failed child should have its own title attribute in its status cell
    assert '<span class="failed" title="child_failed — Child job failed">failed</span>' in body
    # Batch is now in partially_completed state (one completed, one failed)
    assert '<td>partially_completed</td>' in body
    # The aggregate counts cell should not have a title attribute—it shows "1/2 done" with no title
    assert '1/2 done' in body
    # Verify no title attribute exists on the aggregate count cell itself (should be plain text, not a span with title)
    import re
    batch_row = re.search(r'<tr>\s+<td>.*?PLtest.*?</td>\s+<td>partially_completed</td>\s+<td>(.*?)</td>\s+<td></td>\s+</tr>', body, re.DOTALL)
    assert batch_row is not None, "Batch row found"
    counts_cell = batch_row.group(1)
    assert 'title=' not in counts_cell, "Aggregate counts cell should not have a title attribute"


async def _tracker_with_two_batches(tmp_path: Path) -> tuple[JobTracker, UUID, UUID]:
    bus = AsyncIOEventEmitter()
    tracker, _conn = await _build_tracker(tmp_path)
    tracker.register(bus)
    batch_x, batch_y = uuid4(), uuid4()
    for batch_id, tag in ((batch_x, "x"), (batch_y, "y")):
        await _emit(bus, BATCH_CREATED, BatchCreated(batch_id, f"https://youtu.be/playlist?list={tag}", AT))
        child = uuid4()
        await _emit(bus, 
            JOB_QUEUED,
            JobQueued(child, batch_id, SourceKind.YOUTUBE, f"https://youtu.be/{tag}-child", None, AT),
        )
        await _emit(bus, BATCH_JOBS_CREATED, BatchJobsCreated(batch_id, (child,), 0, AT))
    await _drain(bus)
    return tracker, batch_x, batch_y


async def test_batch_children_are_collapsed_by_default(tmp_path: Path) -> None:
    tracker = await _tracker_with_a_standalone_job_and_a_batch(tmp_path)

    async with await _client(tracker) as client:
        response = await client.get("/web/queue")

    assert response.status_code == 200
    assert 'class="queue-child"' not in response.text


async def test_open_query_param_expands_exactly_the_named_batch(tmp_path: Path) -> None:
    tracker, batch_x, batch_y = await _tracker_with_two_batches(tmp_path)

    async with await _client(tracker) as client:
        response = await client.get(f"/web/queue?open={batch_x}")

    body = response.text
    assert response.status_code == 200
    assert "https://youtu.be/x-child" in body
    assert "https://youtu.be/y-child" not in body


async def test_unknown_batch_ids_in_open_are_ignored_not_rejected(tmp_path: Path) -> None:
    tracker = await _tracker_with_a_standalone_job_and_a_batch(tmp_path)

    async with await _client(tracker) as client:
        response = await client.get("/web/queue?open=not-a-real-id,also-garbage")

    assert response.status_code == 200
    assert 'class="queue-child"' not in response.text


async def test_queue_fragment_restates_the_open_set_in_its_own_poll_url(tmp_path: Path) -> None:
    tracker = await _tracker_with_a_standalone_job_and_a_batch(tmp_path)

    async with await _client(tracker) as client:
        response = await client.get(f"/web/queue?open={BATCH}")

    assert f'hx-get="/web/queue?open={BATCH}"' in response.text


async def test_submit_response_carries_the_open_set_through(tmp_path: Path) -> None:
    tracker = await _tracker_with_a_standalone_job_and_a_batch(tmp_path)

    async with await _client(tracker) as client:
        response = await client.post(f"/web/youtube?open={BATCH}", data={"url": "not-a-url"})

    assert response.status_code == 200
    body = response.text
    assert body.count('class="queue-child"') == 2
    assert f'hx-get="/web/queue?open={BATCH}"' in body


async def test_page_reload_with_open_query_param_renders_already_expanded(tmp_path: Path) -> None:
    tracker, batch_x, batch_y = await _tracker_with_two_batches(tmp_path)

    async with await _client(tracker) as client:
        response = await client.get(f"/?open={batch_x},{batch_y}")

    body = response.text
    assert response.status_code == 200
    assert "https://youtu.be/x-child" in body
    assert "https://youtu.be/y-child" in body


async def test_a_batch_row_shows_its_playlist_thumbnail_once_expanded(tmp_path: Path) -> None:
    bus = AsyncIOEventEmitter()
    tracker, _conn = await _build_tracker(tmp_path)
    tracker.register(bus)
    thumbnail = "https://i.ytimg.com/vi/aaaaaaaaaaa/hqdefault.jpg"

    await _emit(bus, BATCH_CREATED, BatchCreated(BATCH, "https://youtu.be/playlist?list=x", AT))
    async with await _client(tracker) as client:
        before = await client.get("/web/queue")
        await _emit(
            bus,
            YOUTUBE_PLAYLIST_EXPANDED,
            PlaylistExpanded(BATCH, (), 0, AT, "Rust Fundamentals", thumbnail),
        )
        await _drain(bus)
        after = await client.get("/web/queue")

    assert "batch-thumb" not in before.text
    assert f'<img class="batch-thumb" src="{thumbnail}"' in after.text
