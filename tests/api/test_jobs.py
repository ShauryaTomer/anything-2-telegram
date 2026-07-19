from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from starlette.requests import ClientDisconnect

from anything2telegram.artifacts.storage import ArtifactStorageError
from anything2telegram.domain import (
    BatchSnapshot,
    BatchStatus,
    ErrorInfo,
    JobSnapshot,
    JobStatus,
    SourceKind,
    StagedArtifact,
    UploadReservation,
)
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

    def submit_youtube_video(self, url: str) -> UUID:
        self.calls.append(("video", url))
        return JOB_ID

    def submit_playlist_expansion(self, url: str) -> UUID:
        self.calls.append(("playlist", url))
        return BATCH_ID

    def reserve_local_upload(
        self, filename: str, media_type: str | None, caption: str | None
    ) -> UploadReservation:
        self.calls.append(("reserve", filename, media_type, caption))
        return UploadReservation(
            JOB_ID,
            ARTIFACT_ID,
            Path("/tmp/staged/video.mp4"),
            filename,
            media_type,
            caption,
        )

    def enqueue_reserved_upload(self, staged: StagedArtifact) -> UUID:
        self.calls.append(("enqueue", staged.size_bytes))
        return staged.job_id

    def cancel_reserved_upload(self, job_id: UUID) -> bool:
        self.calls.append(("cancel", job_id))
        self.cancelled.append(job_id)
        return True


class FakeStorage:
    def __init__(self, error: BaseException | None = None) -> None:
        self.error = error

    async def stage(
        self, upload: object, reservation: UploadReservation, max_bytes: int
    ) -> StagedArtifact:
        scheduler.calls.append(("stage", max_bytes))
        if self.error is not None:
            raise self.error
        chunks = []
        while chunk := await upload.read(2):
            chunks.append(chunk)
            scheduler.calls.append(("read", len(chunk)))
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

    def get_job(self, job_id: UUID) -> JobSnapshot | None:
        return self.jobs.get(job_id)

    def get_batch(self, batch_id: UUID) -> BatchSnapshot | None:
        return self.batches.get(batch_id)


class FakeReadiness:
    accepting = True

    def is_accepting(self) -> bool:
        return self.accepting


class FakeTelegram:
    connected = True

    def is_connected(self) -> bool:
        return self.connected


@pytest.fixture(autouse=True)
def dependencies() -> None:
    global scheduler, storage, tracker, readiness, telegram, client
    scheduler = FakeScheduler()
    storage = FakeStorage()
    tracker = FakeTracker()
    readiness = FakeReadiness()
    telegram = FakeTelegram()
    client = TestClient(
        create_jobs_app(
            scheduler=scheduler,
            tracker=tracker,
            storage=storage,
            readiness=readiness,
            telegram=telegram,
            max_upload_bytes=10,
        )
    )


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


@pytest.mark.parametrize(
    ("error", "status", "code", "message"),
    [
        (
            ArtifactStorageError("staging_oversize", "unsafe ignored"),
            413,
            "staging_oversize",
            "Upload exceeds maximum allowed size",
        ),
        (
            ArtifactStorageError("staging_disk_full", "unsafe ignored"),
            507,
            "staging_disk_full",
            "Artifact storage is full",
        ),
        (
            ArtifactStorageError("staging_io_error", "/private/path leaked"),
            500,
            "upload_staging_failed",
            "Upload could not be stored",
        ),
    ],
)
def test_upload_staging_failures_cancel_without_enqueue(
    error: ArtifactStorageError, status: int, code: str, message: str
) -> None:
    storage.error = error

    response = client.post(
        "/jobs/upload", files={"file": ("video.mp4", b"abcdef", "video/mp4")}
    )

    assert response.status_code == status
    assert response.json() == {"detail": {"code": code, "message": message}}
    assert scheduler.cancelled == [JOB_ID]
    assert not any(call[0] == "enqueue" for call in scheduler.calls)


@pytest.mark.parametrize("stage_error", [ClientDisconnect(), FatalStage()])
def test_upload_cancellation_cleans_reservation_and_reraises(
    stage_error: BaseException,
) -> None:
    storage.error = stage_error

    with pytest.raises(type(stage_error)):
        client.post(
            "/jobs/upload",
            files={"file": ("video.mp4", b"abcdef", "video/mp4")},
        )

    assert scheduler.cancelled == [JOB_ID]
    assert not any(call[0] == "enqueue" for call in scheduler.calls)


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
