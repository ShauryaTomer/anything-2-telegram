from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import aiosqlite
import pytest

from anything2telegram.domain import ErrorInfo, JobStatus, SourceKind
from anything2telegram.jobs.repositories import (
    BatchesRepository,
    BatchQueueRepository,
    BatchRow,
    JobQueueRepository,
    JobQueueRow,
    JobsRepository,
    JobRow,
    PlaylistQueueRow,
    open_database,
)


NOW = datetime(2026, 7, 19, 8, 30, tzinfo=UTC)


@pytest.fixture
async def conn(tmp_path: Path) -> aiosqlite.Connection:
    connection = await open_database(tmp_path / "yt2tg.sqlite3")
    yield connection
    await connection.close()


def make_job_row(**overrides: object) -> JobRow:
    values: dict[str, object] = {
        "id": uuid4(),
        "batch_id": None,
        "source_kind": SourceKind.YOUTUBE,
        "source": "https://example.test/watch?v=one",
        "title": None,
        "status": JobStatus.WAITING,
        "artifact_id": None,
        "filename": None,
        "size_bytes": None,
        "telegram_chat_id": None,
        "telegram_message_id": None,
        "error": None,
        "created_at": NOW,
        "updated_at": NOW,
        "staged": False,
        "ready_local_path": None,
        "ready_media_type": None,
        "ready_caption": None,
    }
    return JobRow(**(values | overrides))


def make_batch_row(**overrides: object) -> BatchRow:
    values: dict[str, object] = {
        "id": uuid4(),
        "source_url": "https://example.test/playlist",
        "title": None,
        "jobs_created": False,
        "skipped_entries": 0,
        "error": None,
        "created_at": NOW,
        "updated_at": NOW,
    }
    return BatchRow(**(values | overrides))


async def test_jobs_repository_round_trips_a_row(conn: aiosqlite.Connection) -> None:
    repo = JobsRepository(conn)
    row = make_job_row()

    await repo.insert(row)

    assert await repo.get(row.id) == row


async def test_jobs_repository_unknown_id_returns_none(conn: aiosqlite.Connection) -> None:
    repo = JobsRepository(conn)
    assert await repo.get(uuid4()) is None


async def test_jobs_repository_update_overwrites_the_row(conn: aiosqlite.Connection) -> None:
    repo = JobsRepository(conn)
    row = make_job_row()
    await repo.insert(row)

    updated = make_job_row(
        id=row.id,
        status=JobStatus.COMPLETED,
        artifact_id=uuid4(),
        error=ErrorInfo("boom", "Boom"),
        updated_at=NOW,
    )
    await repo.update(updated)

    assert await repo.get(row.id) == updated


async def test_jobs_repository_lists_by_batch_in_insertion_order(
    conn: aiosqlite.Connection,
) -> None:
    repo = JobsRepository(conn)
    batch_id = uuid4()
    first = make_job_row(batch_id=batch_id)
    second = make_job_row(batch_id=batch_id)
    standalone = make_job_row(batch_id=None)
    await repo.insert(first)
    await repo.insert(second)
    await repo.insert(standalone)

    assert await repo.list_by_batch(batch_id) == (first, second)
    assert await repo.list_standalone() == (standalone,)


async def test_jobs_repository_lists_by_status(conn: aiosqlite.Connection) -> None:
    repo = JobsRepository(conn)
    producing = make_job_row(status=JobStatus.PRODUCING)
    uploading = make_job_row(status=JobStatus.UPLOADING)
    waiting = make_job_row(status=JobStatus.WAITING)
    await repo.insert(producing)
    await repo.insert(uploading)
    await repo.insert(waiting)

    found = await repo.list_by_status((JobStatus.PRODUCING, JobStatus.UPLOADING))

    assert {row.id for row in found} == {producing.id, uploading.id}


async def test_batches_repository_round_trips_a_row(conn: aiosqlite.Connection) -> None:
    repo = BatchesRepository(conn)
    row = make_batch_row()

    await repo.insert(row)

    assert await repo.get(row.id) == row


async def test_batches_repository_update_overwrites_the_row(
    conn: aiosqlite.Connection,
) -> None:
    repo = BatchesRepository(conn)
    row = make_batch_row()
    await repo.insert(row)

    updated = make_batch_row(
        id=row.id, jobs_created=True, skipped_entries=3, title="Rust Fundamentals"
    )
    await repo.update(updated)

    assert await repo.get(row.id) == updated


async def test_batches_repository_lists_everything(conn: aiosqlite.Connection) -> None:
    repo = BatchesRepository(conn)
    first = make_batch_row()
    second = make_batch_row()
    await repo.insert(first)
    await repo.insert(second)

    listed = await repo.list_all()
    assert len(listed) == 2
    assert first in listed
    assert second in listed


async def test_job_queue_dequeues_in_insertion_order(conn: aiosqlite.Connection) -> None:
    repo = JobQueueRepository(conn)
    first = JobQueueRow(job_id=uuid4(), kind="youtube", source_url="https://a")
    second = JobQueueRow(job_id=uuid4(), kind="youtube", source_url="https://b")
    await repo.enqueue(first)
    await repo.enqueue(second)

    assert await repo.dequeue() == first
    assert await repo.dequeue() == second
    assert await repo.dequeue() is None


async def test_job_queue_prepend_jumps_new_entries_ahead_of_existing_ones(
    conn: aiosqlite.Connection,
) -> None:
    repo = JobQueueRepository(conn)
    later = JobQueueRow(job_id=uuid4(), kind="youtube", source_url="https://later")
    await repo.enqueue(later)

    child_1 = JobQueueRow(job_id=uuid4(), kind="youtube", source_url="https://child1")
    child_2 = JobQueueRow(job_id=uuid4(), kind="youtube", source_url="https://child2")
    await repo.prepend([child_1, child_2])

    assert await repo.dequeue() == child_1
    assert await repo.dequeue() == child_2
    assert await repo.dequeue() == later


async def test_job_queue_remove_drops_a_specific_entry(conn: aiosqlite.Connection) -> None:
    repo = JobQueueRepository(conn)
    keep = JobQueueRow(job_id=uuid4(), kind="staged")
    drop = JobQueueRow(job_id=uuid4(), kind="staged")
    await repo.enqueue(keep)
    await repo.enqueue(drop)

    await repo.remove(drop.job_id)

    assert await repo.list_all() == (keep,)


async def test_job_queue_round_trips_staged_work_fields(conn: aiosqlite.Connection) -> None:
    repo = JobQueueRepository(conn)
    row = JobQueueRow(
        job_id=uuid4(),
        kind="staged",
        artifact_id=uuid4(),
        local_path=Path("/private/artifacts/clip.mp4"),
        filename="clip.mp4",
        media_type="video/mp4",
        size_bytes=123,
        caption="hi",
    )
    await repo.enqueue(row)

    assert await repo.dequeue() == row


async def test_batch_queue_dequeues_in_insertion_order(conn: aiosqlite.Connection) -> None:
    repo = BatchQueueRepository(conn)
    first = PlaylistQueueRow(batch_id=uuid4(), source_url="https://a", offset=0)
    second = PlaylistQueueRow(batch_id=uuid4(), source_url="https://b", offset=2)
    await repo.enqueue(first)
    await repo.enqueue(second)

    assert await repo.dequeue() == first
    assert await repo.dequeue() == second
    assert await repo.dequeue() is None


async def test_job_queue_and_batch_queue_share_a_single_sequence(
    conn: aiosqlite.Connection,
) -> None:
    jobs = JobQueueRepository(conn)
    batches = BatchQueueRepository(conn)
    video = JobQueueRow(job_id=uuid4(), kind="youtube", source_url="https://a")
    playlist = PlaylistQueueRow(batch_id=uuid4(), source_url="https://b")
    await jobs.enqueue(video)
    await batches.enqueue(playlist)

    assert await jobs.peek_sequence() < await batches.peek_sequence()


async def test_a_batchs_thumbnail_round_trips(conn: aiosqlite.Connection) -> None:
    repo = BatchesRepository(conn)
    row = make_batch_row(thumbnail_url="https://i.ytimg.com/vi/aaaaaaaaaaa/hq.jpg")

    await repo.insert(row)
    row.thumbnail_url = "https://i.ytimg.com/vi/bbbbbbbbbbb/hq.jpg"
    await repo.update(row)

    assert await repo.get(row.id) == row


async def test_a_database_predating_the_thumbnail_column_gains_it(
    tmp_path: Path,
) -> None:
    """The column is added by ALTER, so an existing install keeps its history."""
    db_path = tmp_path / "old.sqlite3"
    legacy = await aiosqlite.connect(db_path)
    await legacy.execute(
        """
        CREATE TABLE batches (
            id TEXT PRIMARY KEY, source_url TEXT NOT NULL, title TEXT,
            jobs_created INTEGER NOT NULL DEFAULT 0,
            skipped_entries INTEGER NOT NULL DEFAULT 0,
            error_code TEXT, error_message TEXT,
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        )
        """
    )
    old = make_batch_row()
    await legacy.execute(
        "INSERT INTO batches (id, source_url, title, jobs_created, skipped_entries,"
        " created_at, updated_at) VALUES (?, ?, ?, 0, 0, ?, ?)",
        (str(old.id), old.source_url, old.title, NOW.isoformat(), NOW.isoformat()),
    )
    await legacy.commit()
    await legacy.close()

    conn = await open_database(db_path)
    repo = BatchesRepository(conn)
    stored = await repo.get(old.id)
    stored.thumbnail_url = "https://i.ytimg.com/vi/aaaaaaaaaaa/hq.jpg"
    await repo.update(stored)
    reread = await repo.get(old.id)
    await conn.close()

    assert stored.thumbnail_url == reread.thumbnail_url
