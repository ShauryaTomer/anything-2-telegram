from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest
import starlette.formparsers as formparsers
from fastapi.testclient import TestClient
from starlette.requests import ClientDisconnect, Request

from anything2telegram.artifacts.storage import ArtifactStorageError
from anything2telegram.domain import (
    BatchRef,
    BatchSnapshot,
    BatchStatus,
    ErrorInfo,
    JobRef,
    JobSnapshot,
    JobStatus,
    SourceKind,
    StagedArtifact,
    UploadReservation,
)
from anything2telegram.jobs.scheduler import SchedulerError
from api.jobs import create_jobs_app


JOB_ID = UUID("10000000-0000-0000-0000-000000000001")
BATCH_ID = UUID("20000000-0000-0000-0000-000000000001")
ARTIFACT_ID = UUID("30000000-0000-0000-0000-000000000001")
NOW = datetime(2026, 7, 19, 8, 30, tzinfo=UTC)


class FatalStage(BaseException):
    pass


class FakeScheduler:
    def __init__(self) -> None:
        self.calls: list[object] = []
        self.cancelled: list[UUID] = []
        self.reserve_error: Exception | None = None
        self.submission_error: Exception | None = None
        self.enqueue_error: Exception | None = None
        self.cancel_result = True
        self.cancel_error: Exception | None = None
        self.accepting = True

    def submit_video(self, url: str) -> JobRef:
        self.calls.append(("video", url))
        if self.submission_error is not None:
            raise self.submission_error
        return JobRef(JOB_ID, f"/jobs/{JOB_ID}")

    def submit_playlist(self, url: str) -> BatchRef:
        self.calls.append(("playlist", url))
        if self.submission_error is not None:
            raise self.submission_error
        return BatchRef(BATCH_ID, f"/batches/{BATCH_ID}")

    def reserve_local_upload(
        self, filename: str, media_type: str | None, caption: str | None
    ) -> UploadReservation:
        self.calls.append(("reserve", filename, media_type, caption))
        if self.reserve_error is not None:
            raise self.reserve_error
        return UploadReservation(
            JOB_ID,
            ARTIFACT_ID,
            Path("/tmp/staged/video.mp4"),
            filename,
            media_type,
            caption,
        )

    def enqueue_reserved_upload(
        self, reservation: UploadReservation, size_bytes: int
    ) -> JobRef:
        self.calls.append(("enqueue", size_bytes))
        if self.enqueue_error is not None:
            self.accepting = False
            raise self.enqueue_error
        return JobRef(reservation.job_id, f"/jobs/{reservation.job_id}")

    def cancel_reserved_upload(self, job_id: UUID) -> bool:
        self.calls.append(("cancel", job_id))
        self.cancelled.append(job_id)
        if self.cancel_error is not None:
            raise self.cancel_error
        return self.cancel_result


class FakeStorage:
    def __init__(self, error: BaseException | None = None) -> None:
        self.error = error
        self.upload: object | None = None
        self.lose_readiness = False

    async def stage(
        self, upload: object, reservation: UploadReservation, max_bytes: int
    ) -> StagedArtifact:
        scheduler.calls.append(("stage", max_bytes))
        self.upload = getattr(upload, "_upload", upload)
        if self.error is not None:
            raise self.error
        chunks = []
        while chunk := await upload.read(2):
            chunks.append(chunk)
            scheduler.calls.append(("read", len(chunk)))
        if self.lose_readiness:
            readiness.accepting = False
        return StagedArtifact(
            reservation.job_id,
            reservation.artifact_id,
            reservation.destination,
            reservation.filename,
            reservation.media_type,
            sum(map(len, chunks)),
            reservation.caption,
        )


class FakeTracker:
    def __init__(self) -> None:
        self.jobs: dict[UUID, JobSnapshot] = {}
        self.batches: dict[UUID, BatchSnapshot] = {}
        self.error: Exception | None = None

    def get_job(self, job_id: UUID) -> JobSnapshot | None:
        if self.error is not None:
            raise self.error
        return self.jobs.get(job_id)

    def get_batch(self, batch_id: UUID) -> BatchSnapshot | None:
        return self.batches.get(batch_id)


class FakeReadiness:
    accepting = True
    responses: list[bool] = []

    def is_accepting(self) -> bool:
        if self.responses:
            return self.responses.pop(0)
        return self.accepting


class FakeTelegram:
    connected = True

    def is_connected(self) -> bool:
        return self.connected


@pytest.fixture(autouse=True)
def dependencies() -> None:
    global scheduler, storage, tracker, readiness, telegram, app, client
    scheduler = FakeScheduler()
    storage = FakeStorage()
    tracker = FakeTracker()
    readiness = FakeReadiness()
    telegram = FakeTelegram()
    app = create_jobs_app(
        scheduler=scheduler,
        tracker=tracker,
        storage=storage,
        readiness=readiness,
        telegram=telegram,
        max_upload_bytes=10,
    )
    client = TestClient(app)


@pytest.mark.parametrize(
    ("url", "call", "body"),
    [
        (
            "https://youtu.be/abcdefghijk",
            "video",
            {
                "type": "job",
                "id": str(JOB_ID),
                "status_url": f"/jobs/{JOB_ID}",
            },
        ),
        (
            "https://www.youtube.com/playlist?list=PL123",
            "playlist",
            {
                "type": "batch",
                "id": str(BATCH_ID),
                "status_url": f"/batches/{BATCH_ID}",
            },
        ),
    ],
)
def test_youtube_submission_classifies_and_returns_exact_202(
    url: str, call: str, body: dict[str, str]
) -> None:
    response = client.post("/jobs/youtube", json={"url": url})

    assert response.status_code == 202
    assert response.json() == body
    assert scheduler.calls == [(call, url)]


def test_youtube_validation_and_readiness_have_stable_errors() -> None:
    cases = [
        (
            {"content": "{", "headers": {"content-type": "application/json"}},
            422,
            "invalid_request",
        ),
        ({"json": {}}, 422, "invalid_request"),
        (
            {"json": {"url": "https://example.test/video"}},
            422,
            "unsupported_youtube_url",
        ),
    ]
    for kwargs, status, code in cases:
        response = client.post("/jobs/youtube", **kwargs)
        assert response.status_code == status
        assert response.json()["detail"]["code"] == code
        assert set(response.json()) == {"detail"}
        assert set(response.json()["detail"]) == {"code", "message"}

    readiness.accepting = False
    response = client.post(
        "/jobs/youtube", json={"url": "https://youtu.be/abcdefghijk"}
    )
    assert response.status_code == 503
    assert response.json() == {
        "detail": {
            "code": "service_unavailable",
            "message": "Service is not ready",
        }
    }
    assert scheduler.calls == []

    readiness.accepting = True
    scheduler.submission_error = SchedulerError(
        "scheduler_unavailable", "unsafe ignored"
    )
    response = client.post(
        "/jobs/youtube", json={"url": "https://youtu.be/abcdefghijk"}
    )
    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "service_unavailable"

    framework_errors = [
        (client.get("/unknown"), 404, "not_found", "Resource not found"),
        (
            client.delete("/health"),
            405,
            "method_not_allowed",
            "Method not allowed",
        ),
    ]
    for response, status, code, message in framework_errors:
        assert response.status_code == status
        assert response.json() == {
            "detail": {"code": code, "message": message}
        }
    assert framework_errors[1][0].headers["allow"] == "GET"

    readiness.accepting = True
    malformed = client.post(
        "/jobs/upload",
        content=b"broken",
        headers={"content-type": "multipart/form-data"},
    )
    assert malformed.status_code == 422
    assert malformed.json() == {
        "detail": {"code": "invalid_request", "message": "Request is invalid"}
    }

    tracker.error = RuntimeError("/private/path must not leak")
    response = TestClient(app, raise_server_exceptions=False).get(f"/jobs/{JOB_ID}")
    assert response.status_code == 500
    assert response.json() == {
        "detail": {
            "code": "internal_error",
            "message": "Internal server error",
        }
    }


def test_upload_streams_stage_before_enqueue_and_returns_exact_202() -> None:
    response = client.post(
        "/jobs/upload",
        files={"file": ("video.mp4", b"abcdef", "video/mp4")},
        data={"caption": "Demo"},
    )

    assert response.status_code == 202
    assert response.json() == {
        "type": "job",
        "id": str(JOB_ID),
        "status_url": f"/jobs/{JOB_ID}",
    }
    assert scheduler.calls == [
        ("reserve", "video.mp4", "video/mp4", "Demo"),
        ("stage", 10),
        ("read", 2),
        ("read", 2),
        ("read", 2),
        ("enqueue", 6),
    ]
    assert storage.upload.file.closed

    scheduler.calls.clear()
    response = client.post(
        "/jobs/upload",
        files=[
            ("file", ("one.mp4", b"one", "video/mp4")),
            ("file", ("two.mp4", b"two", "video/mp4")),
        ],
    )
    assert response.status_code == 422
    assert scheduler.calls == []

    readiness.responses = [True, False]
    response = client.post(
        "/jobs/upload",
        files={"file": ("video.mp4", b"small", "video/mp4")},
    )
    assert response.status_code == 503
    assert scheduler.calls == []

    readiness.accepting = True
    storage.lose_readiness = True
    response = client.post(
        "/jobs/upload",
        files={"file": ("video.mp4", b"small", "video/mp4")},
    )
    assert response.status_code == 503
    assert scheduler.cancelled == [JOB_ID]
    assert not any(call[0] == "enqueue" for call in scheduler.calls)

    readiness.accepting = True
    storage.lose_readiness = False
    scheduler.calls.clear()
    response = client.post(
        "/jobs/upload",
        files={"file": ("large.mp4", b"01234567890", "video/mp4")},
    )
    assert response.status_code == 413
    assert response.json() == {
        "detail": {
            "code": "staging_oversize",
            "message": "Upload exceeds maximum allowed size",
        }
    }
    assert scheduler.calls == []


@pytest.mark.parametrize(
    ("phase", "error", "status", "code", "message"),
    [
        (
            "stage",
            ArtifactStorageError("staging_oversize", "unsafe ignored"),
            413,
            "staging_oversize",
            "Upload exceeds maximum allowed size",
        ),
        (
            "stage",
            ArtifactStorageError("staging_disk_full", "unsafe ignored"),
            507,
            "staging_disk_full",
            "Artifact storage is full",
        ),
        (
            "stage",
            ArtifactStorageError("staging_io_error", "/private/path leaked"),
            500,
            "upload_staging_failed",
            "Upload could not be stored",
        ),
        (
            "reserve",
            ArtifactStorageError("invalid_filename", "unsafe ignored"),
            422,
            "invalid_filename",
            "Artifact filename is invalid",
        ),
        (
            "reserve",
            ArtifactStorageError("staging_disk_full", "unsafe ignored"),
            507,
            "staging_disk_full",
            "Artifact storage is full",
        ),
        (
            "reserve",
            ArtifactStorageError("storage_collision", "/private/path leaked"),
            500,
            "upload_reservation_failed",
            "Upload could not be reserved",
        ),
        (
            "enqueue",
            SchedulerError("scheduler_unavailable", "unsafe ignored"),
            503,
            "service_unavailable",
            "Service is not ready",
        ),
    ],
)
def test_upload_staging_failures_cancel_without_enqueue(
    phase: str,
    error: Exception,
    status: int,
    code: str,
    message: str,
) -> None:
    if phase == "reserve":
        scheduler.reserve_error = error
    elif phase == "enqueue":
        error.committed = True
        scheduler.enqueue_error = error
    else:
        storage.error = error

    response = client.post(
        "/jobs/upload", files={"file": ("video.mp4", b"abcdef", "video/mp4")}
    )

    assert response.status_code == status
    assert response.json() == {"detail": {"code": code, "message": message}}
    assert scheduler.cancelled == ([JOB_ID] if phase == "stage" else [])
    assert (phase == "enqueue") is any(
        call[0] == "enqueue" for call in scheduler.calls
    )


@pytest.mark.parametrize("failure", ["post_stage_disconnect", "fatal_stage"])
def test_upload_cancellation_cleans_reservation_and_reraises(
    failure: str,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if failure == "post_stage_disconnect":
        disconnect_checks = iter((False, False, False, False, True))

        async def disconnected(_request: Request) -> bool:
            return next(disconnect_checks)

        monkeypatch.setattr(Request, "is_disconnected", disconnected)
        expected_error = ClientDisconnect
    else:
        storage.error = FatalStage()
        expected_error = FatalStage
    scheduler.cancel_result = False

    with pytest.raises(expected_error):
        client.post(
            "/jobs/upload",
            files={"file": ("video.mp4", b"abcdef", "video/mp4")},
        )

    assert scheduler.cancelled == [JOB_ID]
    assert not any(call[0] == "enqueue" for call in scheduler.calls)
    assert "Upload reservation cleanup failed" in caplog.text


async def test_upload_request_disconnect_closes_parser_temp_file(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created_files: list[object] = []
    original = formparsers.SpooledTemporaryFile

    def recording_temp_file(*args: object, **kwargs: object) -> object:
        temporary_file = original(*args, **kwargs)
        created_files.append(temporary_file)
        return temporary_file

    monkeypatch.setattr(formparsers, "SpooledTemporaryFile", recording_temp_file)
    boundary = b"bounded-test"
    partial_body = (
        b"--" + boundary + b"\r\n"
        b'Content-Disposition: form-data; name="file"; filename="video.mp4"\r\n'
        b"Content-Type: video/mp4\r\n\r\nabc"
    )
    messages = iter(
        [
            {"type": "http.request", "body": partial_body, "more_body": True},
            {"type": "http.disconnect"},
        ]
    )

    async def receive() -> dict[str, object]:
        return next(messages)

    async def send(_message: dict[str, object]) -> None:
        pass

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/jobs/upload",
        "raw_path": b"/jobs/upload",
        "query_string": b"",
        "headers": [
            ("content-type".encode(), b"multipart/form-data; boundary=" + boundary)
        ],
        "client": ("test", 1),
        "server": ("testserver", 80),
    }

    with pytest.raises(ClientDisconnect):
        await app(scope, receive, send)

    assert created_files
    assert all(file.closed for file in created_files)
    assert scheduler.calls == []


def test_job_batch_snapshots_are_exact_and_bad_ids_are_404() -> None:
    tracker.jobs[JOB_ID] = JobSnapshot(
        JOB_ID,
        BATCH_ID,
        SourceKind.YOUTUBE,
        "https://youtu.be/abcdefghijk",
        JobStatus.FAILED,
        ARTIFACT_ID,
        "video.mp4",
        123,
        None,
        ErrorInfo("download_failed", "Download failed"),
        NOW,
        NOW,
    )
    tracker.batches[BATCH_ID] = BatchSnapshot(
        BATCH_ID,
        "https://www.youtube.com/playlist?list=PL123",
        BatchStatus.PARTIALLY_COMPLETED,
        (JOB_ID,),
        2,
        None,
        NOW,
        NOW,
        1,
        0,
        0,
        0,
        0,
        1,
    )

    assert client.get(f"/jobs/{JOB_ID}").json() == {
        "id": str(JOB_ID),
        "batch_id": str(BATCH_ID),
        "source_kind": "youtube",
        "source": "https://youtu.be/abcdefghijk",
        "status": "failed",
        "artifact_id": str(ARTIFACT_ID),
        "filename": "video.mp4",
        "size_bytes": 123,
        "telegram_message_id": None,
        "error": {"code": "download_failed", "message": "Download failed"},
        "created_at": "2026-07-19T08:30:00Z",
        "updated_at": "2026-07-19T08:30:00Z",
    }
    assert client.get(f"/batches/{BATCH_ID}").json() == {
        "id": str(BATCH_ID),
        "source_url": "https://www.youtube.com/playlist?list=PL123",
        "status": "partially_completed",
        "total_jobs": 1,
        "waiting": 0,
        "producing": 0,
        "uploading": 0,
        "completed": 0,
        "failed": 1,
        "skipped_entries": 2,
        "job_ids": [str(JOB_ID)],
        "error": None,
        "created_at": "2026-07-19T08:30:00Z",
        "updated_at": "2026-07-19T08:30:00Z",
    }
    for path in ("/jobs/not-a-uuid", f"/jobs/{UUID(int=9)}", "/batches/nope"):
        response = client.get(path)
        assert response.status_code == 404
        assert set(response.json()["detail"]) == {"code", "message"}


def test_health_is_exact_and_tracks_runtime_disconnect() -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {
        "ready": True,
        "telegram_connected": True,
    }

    telegram.connected = False
    response = client.get("/health")
    assert response.status_code == 503
    assert response.json() == {
        "ready": False,
        "telegram_connected": False,
    }
    response = client.post(
        "/jobs/youtube", json={"url": "https://youtu.be/abcdefghijk"}
    )
    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "service_unavailable"

    telegram.connected = True
    readiness.accepting = False
    response = client.get("/health")
    assert response.status_code == 503
    assert response.json() == {
        "ready": False,
        "telegram_connected": True,
    }

    readiness.accepting = True
    scheduler.accepting = False
    response = client.get("/health")
    assert response.status_code == 503
    assert response.json() == {
        "ready": False,
        "telegram_connected": True,
    }
    scheduler.calls.clear()
    response = client.post(
        "/jobs/upload",
        files={"file": ("video.mp4", b"small", "video/mp4")},
    )
    assert response.status_code == 503
    assert scheduler.calls == []
    JobRef,
