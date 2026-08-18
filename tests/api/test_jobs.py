from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from httpx import ASGITransport, AsyncClient

from anything2telegram.api.jobs import create_jobs_app
from anything2telegram.artifacts.storage import ArtifactStorageError
from anything2telegram.domain import (
    BatchEntry,
    BatchRef,
    BatchSnapshot,
    BatchStatus,
    ErrorInfo,
    JobPhase,
    JobRef,
    JobSnapshot,
    JobStatus,
    SourceKind,
    StagedArtifact,
    UploadReservation,
)
from anything2telegram.jobs.progress import Progress
from anything2telegram.jobs.scheduler import SchedulerError


AT = datetime(2026, 7, 26, 12, 0, tzinfo=UTC)
VIDEO = "https://youtu.be/abcdefghijk"
PLAYLIST = "https://www.youtube.com/playlist?list=PL123"


class FakeScheduler:
    def __init__(self, *, accepting: bool = True) -> None:
        self.accepting = accepting
        self.calls: list[tuple[str, object]] = []
        self.cancelled: list[UUID] = []
        self.reserve_error: BaseException | None = None
        self.enqueue_error: BaseException | None = None
        self.submit_error: BaseException | None = None

    async def submit_video(self, url: str) -> JobRef:
        self.calls.append(("video", url))
        if self.submit_error is not None:
            raise self.submit_error
        return JobRef(UUID(int=1), f"/jobs/{UUID(int=1)}")

    async def submit_playlist(self, url: str, offset: int = 0) -> BatchRef:
        self.calls.append(("playlist", (url, offset)))
        if self.submit_error is not None:
            raise self.submit_error
        return BatchRef(UUID(int=2), f"/batches/{UUID(int=2)}")

    async def reserve_local_upload(
        self, filename: str, media_type: str | None, caption: str | None
    ) -> UploadReservation:
        self.calls.append(("reserve", (filename, media_type, caption)))
        if self.reserve_error is not None:
            raise self.reserve_error
        return UploadReservation(
            UUID(int=3),
            UUID(int=4),
            Path("/tmp/artifacts") / str(UUID(int=3)) / str(UUID(int=4)) / filename,
            filename,
            media_type,
            caption,
        )

    async def enqueue_reserved_upload(
        self, reservation: UploadReservation, size_bytes: int
    ) -> JobRef:
        self.calls.append(("enqueue", size_bytes))
        if self.enqueue_error is not None:
            raise self.enqueue_error
        return JobRef(reservation.job_id, f"/jobs/{reservation.job_id}")

    async def cancel_reserved_upload(self, job_id: UUID) -> bool:
        self.cancelled.append(job_id)
        return True


class FakeStorage:
    def __init__(self) -> None:
        self.error: BaseException | None = None
        self.staged_bytes = b""

    async def stage(
        self, upload: object, reservation: UploadReservation, max_bytes: int
    ) -> StagedArtifact:
        chunks = []
        while True:
            chunk = await upload.read(64 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        self.staged_bytes = b"".join(chunks)
        if self.error is not None:
            raise self.error
        return StagedArtifact(
            reservation.job_id,
            reservation.artifact_id,
            reservation.destination,
            reservation.filename,
            reservation.media_type,
            len(self.staged_bytes),
            reservation.caption,
        )


class FakeTracker:
    def __init__(self) -> None:
        self.jobs: dict[UUID, JobSnapshot] = {}
        self.batches: dict[UUID, BatchSnapshot] = {}
        self.queue: tuple[JobSnapshot | BatchEntry, ...] = ()

    async def get_job(self, job_id: UUID) -> JobSnapshot | None:
        return self.jobs.get(job_id)

    async def get_batch(self, batch_id: UUID) -> BatchSnapshot | None:
        return self.batches.get(batch_id)

    async def list_queue(self) -> tuple[JobSnapshot | BatchEntry, ...]:
        return self.queue


class FakeProgressRegistry:
    def __init__(self) -> None:
        self.values: dict[UUID, Progress] = {}

    def get(self, job_id: UUID) -> Progress | None:
        return self.values.get(job_id)


@pytest.fixture
def api():
    app = create_jobs_app()
    state = app.state
    state.settings = SimpleNamespace(max_artifact_bytes=1024, topic_id=None)
    state.readiness = SimpleNamespace(is_accepting=lambda: True)
    state.telegram = SimpleNamespace(is_connected=True)
    state.scheduler = FakeScheduler()
    state.storage = FakeStorage()
    state.tracker = FakeTracker()
    state.progress = FakeProgressRegistry()
    return app


@pytest.fixture
async def client(api):
    transport = ASGITransport(app=api)
    async with AsyncClient(transport=transport, base_url="http://service") as client:
        yield client


def detail(response) -> dict[str, str]:
    return response.json()["detail"]


async def test_a_video_url_is_submitted_as_a_job(client, api) -> None:
    response = await client.post("/jobs/youtube", json={"url": VIDEO})

    assert response.status_code == 202
    assert response.json() == {
        "type": "job",
        "id": str(UUID(int=1)),
        "status_url": f"/jobs/{UUID(int=1)}",
    }
    assert api.state.scheduler.calls == [("video", VIDEO)]


async def test_a_playlist_url_is_submitted_as_a_batch_with_its_offset(
    client, api
) -> None:
    response = await client.post(
        "/jobs/youtube", json={"url": PLAYLIST, "offset": 5}
    )

    assert response.status_code == 202
    assert response.json()["type"] == "batch"
    assert api.state.scheduler.calls == [("playlist", (PLAYLIST, 5))]


@pytest.mark.parametrize(
    "body,code",
    [
        ({"url": "https://example.com/watch?v=abcdefghijk"}, "unsupported_youtube_url"),
        ({"url": ""}, "unsupported_youtube_url"),
        ({"url": VIDEO, "offset": -1}, "invalid_request"),
        ({"url": VIDEO, "extra": 1}, "invalid_request"),
        ({}, "invalid_request"),
    ],
)
async def test_bad_youtube_requests_are_rejected_before_the_scheduler(
    client, api, body: dict[str, object], code: str
) -> None:
    response = await client.post("/jobs/youtube", json=body)

    assert response.status_code == 422
    assert detail(response)["code"] == code
    assert api.state.scheduler.calls == []


@pytest.mark.parametrize(
    "attribute,value",
    [
        ("readiness", SimpleNamespace(is_accepting=lambda: False)),
        ("telegram", SimpleNamespace(is_connected=False)),
    ],
)
async def test_submission_is_refused_while_the_service_is_not_ready(
    client, api, attribute: str, value: object
) -> None:
    setattr(api.state, attribute, value)

    response = await client.post("/jobs/youtube", json={"url": VIDEO})

    assert response.status_code == 503
    assert detail(response) == {
        "code": "service_unavailable",
        "message": "Service is not ready",
    }


async def test_a_stopping_scheduler_is_reported_as_unavailable(client, api) -> None:
    api.state.scheduler.submit_error = SchedulerError(
        "scheduler_unavailable", "Scheduler is unavailable"
    )

    response = await client.post("/jobs/youtube", json={"url": VIDEO})

    assert response.status_code == 503


async def test_an_upload_is_staged_then_enqueued(client, api) -> None:
    response = await client.post(
        "/jobs/upload",
        files={"file": ("my clip.mp4", b"payload", "video/mp4")},
        data={"caption": "hello"},
    )

    assert response.status_code == 202
    assert response.json()["status_url"] == f"/jobs/{UUID(int=3)}"
    assert api.state.storage.staged_bytes == b"payload"
    assert api.state.scheduler.calls == [
        ("reserve", ("my clip.mp4", "video/mp4", "hello")),
        ("enqueue", 7),
    ]
    assert api.state.scheduler.cancelled == []


async def test_an_upload_without_a_caption_is_accepted(client, api) -> None:
    response = await client.post(
        "/jobs/upload", files={"file": ("clip.mp4", b"payload", "video/mp4")}
    )

    assert response.status_code == 202
    assert api.state.scheduler.calls[0] == ("reserve", ("clip.mp4", "video/mp4", None))


@pytest.mark.parametrize(
    "files,data",
    [
        ({}, {"caption": "no file"}),
        ({"wrong": ("clip.mp4", b"x", "video/mp4")}, {}),
        ({"file": ("", b"x", "video/mp4")}, {}),
        ({"file": ("clip.mp4", b"x", "video/mp4")}, {"note": "unknown field"}),
    ],
)
async def test_malformed_upload_forms_are_rejected(
    client, api, files: dict, data: dict
) -> None:
    response = await client.post(
        "/jobs/upload", files=files or None, data=data or None
    )

    assert response.status_code == 422
    assert detail(response)["code"] == "invalid_request"
    assert api.state.scheduler.calls == []


async def test_an_upload_larger_than_the_limit_is_refused_before_reading(
    client, api
) -> None:
    api.state.settings = SimpleNamespace(max_artifact_bytes=4)

    response = await client.post(
        "/jobs/upload", files={"file": ("clip.mp4", b"much too long", "video/mp4")}
    )

    assert response.status_code == 413
    assert detail(response)["code"] == "staging_oversize"
    assert api.state.scheduler.calls == []


@pytest.mark.parametrize(
    "code,status,expected_code",
    [
        ("staging_oversize", 413, "staging_oversize"),
        ("staging_disk_full", 507, "staging_disk_full"),
        ("staging_io_error", 500, "upload_staging_failed"),
    ],
)
async def test_a_staging_failure_cancels_the_reservation(
    client, api, code: str, status: int, expected_code: str
) -> None:
    api.state.storage.error = ArtifactStorageError(code, "nope")

    response = await client.post(
        "/jobs/upload", files={"file": ("clip.mp4", b"payload", "video/mp4")}
    )

    assert response.status_code == status
    assert detail(response)["code"] == expected_code
    assert ("enqueue", 7) not in api.state.scheduler.calls


async def test_an_unusable_filename_is_reported_by_the_reservation(client, api) -> None:
    api.state.scheduler.reserve_error = ArtifactStorageError(
        "invalid_filename", "Artifact filename is invalid"
    )

    response = await client.post(
        "/jobs/upload", files={"file": ("...", b"payload", "video/mp4")}
    )

    assert response.status_code == 422
    assert detail(response)["code"] == "invalid_filename"


async def test_a_scheduler_that_stops_mid_upload_cancels_the_reservation(
    client, api
) -> None:
    api.state.scheduler.enqueue_error = SchedulerError(
        "scheduler_unavailable", "Scheduler is unavailable"
    )

    response = await client.post(
        "/jobs/upload", files={"file": ("clip.mp4", b"payload", "video/mp4")}
    )

    assert response.status_code == 503
    assert api.state.scheduler.cancelled == [UUID(int=3)]


async def test_an_unexpected_enqueue_failure_cancels_and_returns_500(api) -> None:
    api.state.scheduler.enqueue_error = RuntimeError("boom")
    transport = ASGITransport(app=api, raise_app_exceptions=False)

    async with AsyncClient(transport=transport, base_url="http://service") as client:
        response = await client.post(
            "/jobs/upload", files={"file": ("clip.mp4", b"payload", "video/mp4")}
        )

    assert response.status_code == 500
    assert detail(response)["code"] == "internal_error"
    assert api.state.scheduler.cancelled == [UUID(int=3)]


async def test_a_job_snapshot_is_rendered_exactly(client, api) -> None:
    job_id, artifact_id = uuid4(), uuid4()
    api.state.tracker.jobs[job_id] = JobSnapshot(
        id=job_id,
        batch_id=None,
        source_kind=SourceKind.YOUTUBE,
        source=VIDEO,
        title=None,
        status=JobStatus.FAILED,
        artifact_id=artifact_id,
        filename="clip.mp4",
        size_bytes=12,
        telegram_chat_id=None,
        telegram_message_id=None,
        error=ErrorInfo("youtube_timeout", "YouTube operation timed out"),
        created_at=AT,
        updated_at=AT,
    )

    response = await client.get(f"/jobs/{job_id}")

    assert response.status_code == 200
    assert response.json() == {
        "id": str(job_id),
        "batch_id": None,
        "source_kind": "youtube",
        "source": VIDEO,
        "status": "failed",
        "artifact_id": str(artifact_id),
        "filename": "clip.mp4",
        "size_bytes": 12,
        "telegram_chat_id": None,
        "telegram_message_id": None,
        "telegram_message_url": None,
        "progress_phase": None,
        "progress_sent": None,
        "progress_total": None,
        "error": {
            "code": "youtube_timeout",
            "message": "YouTube operation timed out",
        },
        "created_at": "2026-07-26T12:00:00Z",
        "updated_at": "2026-07-26T12:00:00Z",
    }


async def test_a_batch_snapshot_is_rendered_exactly(client, api) -> None:
    batch_id, job_id = uuid4(), uuid4()
    api.state.tracker.batches[batch_id] = BatchSnapshot(
        id=batch_id,
        source_url=PLAYLIST,
        title=None,
        status=BatchStatus.PROCESSING,
        job_ids=(job_id,),
        skipped_entries=2,
        error=None,
        created_at=AT,
        updated_at=AT,
        total_jobs=1,
        waiting=0,
        producing=1,
        uploading=0,
        completed=0,
        failed=0,
    )

    response = await client.get(f"/batches/{batch_id}")

    assert response.status_code == 200
    assert response.json() == {
        "id": str(batch_id),
        "source_url": PLAYLIST,
        "status": "processing",
        "total_jobs": 1,
        "waiting": 0,
        "producing": 1,
        "uploading": 0,
        "completed": 0,
        "failed": 0,
        "skipped_entries": 2,
        "job_ids": [str(job_id)],
        "error": None,
        "created_at": "2026-07-26T12:00:00Z",
        "updated_at": "2026-07-26T12:00:00Z",
    }


@pytest.mark.parametrize(
    "path,code",
    [
        ("/jobs/not-a-uuid", "job_not_found"),
        (f"/jobs/{uuid4()}", "job_not_found"),
        ("/batches/not-a-uuid", "batch_not_found"),
        (f"/batches/{uuid4()}", "batch_not_found"),
    ],
)
async def test_unknown_and_malformed_identifiers_are_404(
    client, path: str, code: str
) -> None:
    response = await client.get(path)

    assert response.status_code == 404
    assert detail(response)["code"] == code


async def test_queue_json_is_empty_when_nothing_is_queued(client) -> None:
    response = await client.get("/queue")

    assert response.status_code == 200
    assert response.json() == []


async def test_queue_json_discriminates_jobs_and_batches_by_type(client, api) -> None:
    job_id = uuid4()
    job = JobSnapshot(
        id=job_id,
        batch_id=None,
        source_kind=SourceKind.YOUTUBE,
        source=VIDEO,
        title=None,
        status=JobStatus.WAITING,
        artifact_id=None,
        filename=None,
        size_bytes=None,
        telegram_chat_id=None,
        telegram_message_id=None,
        error=None,
        created_at=AT,
        updated_at=AT,
    )
    batch_id, child_id = uuid4(), uuid4()
    batch = BatchSnapshot(
        id=batch_id,
        source_url=PLAYLIST,
        title=None,
        status=BatchStatus.PROCESSING,
        job_ids=(child_id,),
        skipped_entries=0,
        error=None,
        created_at=AT,
        updated_at=AT,
        total_jobs=1,
        waiting=0,
        producing=1,
        uploading=0,
        completed=0,
        failed=0,
    )
    api.state.tracker.batches[batch_id] = batch
    api.state.tracker.jobs[job_id] = job
    child = JobSnapshot(
        id=child_id,
        batch_id=batch_id,
        source_kind=SourceKind.YOUTUBE,
        source="https://youtu.be/child",
        title=None,
        status=JobStatus.PRODUCING,
        artifact_id=None,
        filename=None,
        size_bytes=None,
        telegram_chat_id=None,
        telegram_message_id=None,
        error=None,
        created_at=AT,
        updated_at=AT,
    )
    api.state.tracker.queue = (BatchEntry(batch, (child,)), job)

    response = await client.get("/queue")
    batch_response = await client.get(f"/batches/{batch_id}")
    job_response = await client.get(f"/jobs/{job_id}")

    assert response.status_code == 200
    batch_row, job_row = response.json()
    assert batch_row == batch_response.json() | {
        "type": "batch",
        "jobs": [
            {
                "id": str(child_id),
                "batch_id": str(batch_id),
                "source_kind": "youtube",
                "source": "https://youtu.be/child",
                "status": "producing",
                "artifact_id": None,
                "filename": None,
                "size_bytes": None,
                "telegram_chat_id": None,
                "telegram_message_id": None,
                "telegram_message_url": None,
                "progress_phase": None,
                "progress_sent": None,
                "progress_total": None,
                "error": None,
                "created_at": "2026-07-26T12:00:00Z",
                "updated_at": "2026-07-26T12:00:00Z",
            }
        ],
    }
    assert job_row == job_response.json() | {"type": "job"}


async def test_job_json_carries_live_progress_from_the_registry(client, api) -> None:
    job_id = uuid4()
    api.state.tracker.jobs[job_id] = JobSnapshot(
        id=job_id,
        batch_id=None,
        source_kind=SourceKind.YOUTUBE,
        source=VIDEO,
        title=None,
        status=JobStatus.PRODUCING,
        artifact_id=None,
        filename=None,
        size_bytes=None,
        telegram_chat_id=None,
        telegram_message_id=None,
        error=None,
        created_at=AT,
        updated_at=AT,
    )
    api.state.progress.values[job_id] = Progress(JobPhase.PRODUCING, 42, 0)

    response = await client.get(f"/jobs/{job_id}")

    body = response.json()
    assert body["progress_phase"] == "producing"
    assert body["progress_sent"] == 42
    assert body["progress_total"] == 0


async def test_job_json_derives_the_telegram_link_only_once_completed(client, api) -> None:
    job_id, artifact_id = uuid4(), uuid4()
    api.state.settings.topic_id = 7
    api.state.tracker.jobs[job_id] = JobSnapshot(
        id=job_id,
        batch_id=None,
        source_kind=SourceKind.YOUTUBE,
        source=VIDEO,
        title=None,
        status=JobStatus.COMPLETED,
        artifact_id=artifact_id,
        filename="clip.mp4",
        size_bytes=12,
        telegram_chat_id=-1001234567890,
        telegram_message_id=55,
        error=None,
        created_at=AT,
        updated_at=AT,
    )

    response = await client.get(f"/jobs/{job_id}")

    body = response.json()
    assert body["telegram_chat_id"] == -1001234567890
    assert body["telegram_message_url"] == "https://t.me/c/1234567890/7/55"


async def test_queue_json_reads_progress_per_child_job_independently(client, api) -> None:
    batch_id, child_a, child_b = uuid4(), uuid4(), uuid4()

    def job(job_id: UUID, status: JobStatus) -> JobSnapshot:
        return JobSnapshot(
            id=job_id,
            batch_id=batch_id,
            source_kind=SourceKind.YOUTUBE,
            source=f"https://youtu.be/{job_id}",
            title=None,
            status=status,
            artifact_id=None,
            filename=None,
            size_bytes=None,
            telegram_chat_id=None,
            telegram_message_id=None,
            error=None,
            created_at=AT,
            updated_at=AT,
        )

    batch = BatchSnapshot(
        id=batch_id,
        source_url=PLAYLIST,
        title=None,
        status=BatchStatus.PROCESSING,
        job_ids=(child_a, child_b),
        skipped_entries=0,
        error=None,
        created_at=AT,
        updated_at=AT,
        total_jobs=2,
        waiting=0,
        producing=1,
        uploading=1,
        completed=0,
        failed=0,
    )
    api.state.tracker.queue = (
        BatchEntry(batch, (job(child_a, JobStatus.PRODUCING), job(child_b, JobStatus.UPLOADING))),
    )
    api.state.progress.values[child_a] = Progress(JobPhase.PRODUCING, 10, 0)
    api.state.progress.values[child_b] = Progress(JobPhase.UPLOADING, 5, 20)

    response = await client.get("/queue")

    [batch_row] = response.json()
    child_a_row, child_b_row = batch_row["jobs"]
    assert (child_a_row["progress_sent"], child_a_row["progress_total"]) == (10, 0)
    assert (child_b_row["progress_sent"], child_b_row["progress_total"]) == (5, 20)


async def test_health_reports_readiness_and_telegram_separately(client, api) -> None:
    ready = await client.get("/health")
    assert ready.status_code == 200
    assert ready.json() == {"ready": True, "telegram_connected": True}

    api.state.telegram = SimpleNamespace(is_connected=False)
    degraded = await client.get("/health")
    assert degraded.status_code == 503
    assert degraded.json() == {"ready": False, "telegram_connected": False}


async def test_health_is_503_before_the_lifespan_has_built_anything() -> None:
    app = create_jobs_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://service") as client:
        response = await client.get("/health")

    assert response.status_code == 503
    assert response.json() == {"ready": False, "telegram_connected": False}


@pytest.mark.parametrize(
    "method,path,status,code",
    [
        ("get", "/missing", 404, "not_found"),
        ("delete", "/health", 405, "method_not_allowed"),
    ],
)
async def test_framework_errors_use_the_same_error_shape(
    client, method: str, path: str, status: int, code: str
) -> None:
    response = await getattr(client, method)(path)

    assert response.status_code == status
    assert detail(response)["code"] == code
