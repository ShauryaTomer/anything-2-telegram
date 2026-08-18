"""SQLite persistence for job/batch history and the pending work queue.

One table per repository. No table owns cross-table logic (e.g. a batch's
job ids are derived by querying ``jobs.batch_id``, not duplicated here) —
that stays in the tracker/scheduler.
"""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from uuid import UUID

import aiosqlite

from ..domain import ErrorInfo, JobStatus, SourceKind


_SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    id TEXT PRIMARY KEY,
    source_url TEXT NOT NULL,
    title TEXT,
    thumbnail_url TEXT,
    jobs_created INTEGER NOT NULL DEFAULT 0,
    skipped_entries INTEGER NOT NULL DEFAULT 0,
    error_code TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    batch_id TEXT,
    source_kind TEXT NOT NULL,
    source TEXT NOT NULL,
    title TEXT,
    status TEXT NOT NULL,
    artifact_id TEXT,
    filename TEXT,
    size_bytes INTEGER,
    telegram_chat_id INTEGER,
    telegram_message_id INTEGER,
    error_code TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    staged INTEGER NOT NULL,
    ready_local_path TEXT,
    ready_media_type TEXT,
    ready_caption TEXT
);

CREATE TABLE IF NOT EXISTS job_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sequence INTEGER NOT NULL,
    job_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    source_url TEXT,
    caption_prefix TEXT,
    title TEXT,
    artifact_id TEXT,
    local_path TEXT,
    filename TEXT,
    media_type TEXT,
    size_bytes INTEGER,
    caption TEXT
);

CREATE TABLE IF NOT EXISTS batch_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sequence INTEGER NOT NULL,
    batch_id TEXT NOT NULL,
    source_url TEXT NOT NULL,
    offset INTEGER NOT NULL DEFAULT 0
);
"""

_NEXT_SEQUENCE_SQL = (
    "SELECT COALESCE(MAX(sequence), 0) + 1 AS next FROM ("
    "SELECT sequence FROM job_queue UNION ALL SELECT sequence FROM batch_queue"
    ")"
)


async def open_database(db_path: Path) -> aiosqlite.Connection:
    """Open (creating if needed) the shared connection and apply the schema."""
    connection = await aiosqlite.connect(db_path)
    connection.row_factory = aiosqlite.Row
    await connection.executescript(_SCHEMA)
    # CREATE TABLE IF NOT EXISTS leaves an older database's batches table as it
    # was, so columns added after that database was created land here.
    try:
        await connection.execute("ALTER TABLE batches ADD COLUMN thumbnail_url TEXT")
    except aiosqlite.OperationalError:
        pass
    await connection.commit()
    return connection


async def _next_sequence(conn: aiosqlite.Connection) -> int:
    """A single counter shared by both queues, so FIFO order spans them.

    Two separate per-table autoincrement counters could not preserve the
    submission order between playlist-expansion work and video/upload work.
    """
    cursor = await conn.execute(_NEXT_SEQUENCE_SQL)
    row = await cursor.fetchone()
    return row["next"]


def _dt(value: datetime) -> str:
    return value.isoformat()


def _parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _uuid_or_none(value: str | None) -> UUID | None:
    return UUID(value) if value is not None else None


def _str_or_none(value: UUID | Path | None) -> str | None:
    return str(value) if value is not None else None


@dataclass
class JobRow:
    id: UUID
    batch_id: UUID | None
    source_kind: SourceKind
    source: str
    title: str | None
    status: JobStatus
    artifact_id: UUID | None
    filename: str | None
    size_bytes: int | None
    telegram_chat_id: int | None
    telegram_message_id: int | None
    error: ErrorInfo | None
    created_at: datetime
    updated_at: datetime
    staged: bool
    ready_local_path: Path | None = None
    ready_media_type: str | None = None
    ready_caption: str | None = None


@dataclass
class BatchRow:
    id: UUID
    source_url: str
    title: str | None
    jobs_created: bool
    skipped_entries: int
    error: ErrorInfo | None
    created_at: datetime
    updated_at: datetime
    thumbnail_url: str | None = None


@dataclass
class JobQueueRow:
    job_id: UUID
    kind: str
    source_url: str | None = None
    caption_prefix: str = ""
    title: str | None = None
    artifact_id: UUID | None = None
    local_path: Path | None = None
    filename: str | None = None
    media_type: str | None = None
    size_bytes: int | None = None
    caption: str | None = None


@dataclass
class PlaylistQueueRow:
    batch_id: UUID
    source_url: str
    offset: int = 0


def _job_from_row(row: aiosqlite.Row) -> JobRow:
    error = (
        ErrorInfo(row["error_code"], row["error_message"])
        if row["error_code"] is not None
        else None
    )
    return JobRow(
        id=UUID(row["id"]),
        batch_id=_uuid_or_none(row["batch_id"]),
        source_kind=SourceKind(row["source_kind"]),
        source=row["source"],
        title=row["title"],
        status=JobStatus(row["status"]),
        artifact_id=_uuid_or_none(row["artifact_id"]),
        filename=row["filename"],
        size_bytes=row["size_bytes"],
        telegram_chat_id=row["telegram_chat_id"],
        telegram_message_id=row["telegram_message_id"],
        error=error,
        created_at=_parse_dt(row["created_at"]),
        updated_at=_parse_dt(row["updated_at"]),
        staged=bool(row["staged"]),
        ready_local_path=(
            Path(row["ready_local_path"]) if row["ready_local_path"] else None
        ),
        ready_media_type=row["ready_media_type"],
        ready_caption=row["ready_caption"],
    )


class JobsRepository:
    def __init__(self, conn: aiosqlite.Connection) -> None:
        self._conn = conn

    async def insert(self, row: JobRow) -> None:
        await self._conn.execute(
            """
            INSERT INTO jobs (
                id, batch_id, source_kind, source, title, status, artifact_id,
                filename, size_bytes, telegram_chat_id, telegram_message_id,
                error_code, error_message, created_at, updated_at, staged,
                ready_local_path, ready_media_type, ready_caption
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            self._params(row),
        )
        await self._conn.commit()

    async def update(self, row: JobRow) -> None:
        params = self._params(row)
        await self._conn.execute(
            """
            UPDATE jobs SET
                batch_id = ?, source_kind = ?, source = ?, title = ?, status = ?,
                artifact_id = ?, filename = ?, size_bytes = ?, telegram_chat_id = ?,
                telegram_message_id = ?, error_code = ?, error_message = ?,
                created_at = ?, updated_at = ?, staged = ?, ready_local_path = ?,
                ready_media_type = ?, ready_caption = ?
            WHERE id = ?
            """,
            (*params[1:], params[0]),
        )
        await self._conn.commit()

    @staticmethod
    def _params(row: JobRow) -> tuple[object, ...]:
        return (
            str(row.id),
            _str_or_none(row.batch_id),
            row.source_kind.value,
            row.source,
            row.title,
            row.status.value,
            _str_or_none(row.artifact_id),
            row.filename,
            row.size_bytes,
            row.telegram_chat_id,
            row.telegram_message_id,
            row.error.code if row.error else None,
            row.error.message if row.error else None,
            _dt(row.created_at),
            _dt(row.updated_at),
            int(row.staged),
            _str_or_none(row.ready_local_path),
            row.ready_media_type,
            row.ready_caption,
        )

    async def get(self, job_id: UUID) -> JobRow | None:
        cursor = await self._conn.execute(
            "SELECT * FROM jobs WHERE id = ?", (str(job_id),)
        )
        row = await cursor.fetchone()
        return _job_from_row(row) if row is not None else None

    async def list_by_batch(self, batch_id: UUID) -> tuple[JobRow, ...]:
        cursor = await self._conn.execute(
            "SELECT * FROM jobs WHERE batch_id = ? ORDER BY rowid", (str(batch_id),)
        )
        rows = await cursor.fetchall()
        return tuple(_job_from_row(row) for row in rows)

    async def list_standalone(self) -> tuple[JobRow, ...]:
        cursor = await self._conn.execute(
            "SELECT * FROM jobs WHERE batch_id IS NULL ORDER BY rowid"
        )
        rows = await cursor.fetchall()
        return tuple(_job_from_row(row) for row in rows)

    async def list_by_status(self, statuses: tuple[JobStatus, ...]) -> tuple[JobRow, ...]:
        placeholders = ", ".join("?" for _ in statuses)
        cursor = await self._conn.execute(
            f"SELECT * FROM jobs WHERE status IN ({placeholders}) ORDER BY rowid",
            tuple(status.value for status in statuses),
        )
        rows = await cursor.fetchall()
        return tuple(_job_from_row(row) for row in rows)


def _batch_from_row(row: aiosqlite.Row) -> BatchRow:
    error = (
        ErrorInfo(row["error_code"], row["error_message"])
        if row["error_code"] is not None
        else None
    )
    return BatchRow(
        id=UUID(row["id"]),
        source_url=row["source_url"],
        title=row["title"],
        thumbnail_url=row["thumbnail_url"],
        jobs_created=bool(row["jobs_created"]),
        skipped_entries=row["skipped_entries"],
        error=error,
        created_at=_parse_dt(row["created_at"]),
        updated_at=_parse_dt(row["updated_at"]),
    )


class BatchesRepository:
    def __init__(self, conn: aiosqlite.Connection) -> None:
        self._conn = conn

    async def insert(self, row: BatchRow) -> None:
        await self._conn.execute(
            """
            INSERT INTO batches (
                id, source_url, title, jobs_created, skipped_entries,
                error_code, error_message, created_at, updated_at, thumbnail_url
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            self._params(row),
        )
        await self._conn.commit()

    async def update(self, row: BatchRow) -> None:
        params = self._params(row)
        await self._conn.execute(
            """
            UPDATE batches SET
                source_url = ?, title = ?, jobs_created = ?, skipped_entries = ?,
                error_code = ?, error_message = ?, created_at = ?, updated_at = ?,
                thumbnail_url = ?
            WHERE id = ?
            """,
            (*params[1:], params[0]),
        )
        await self._conn.commit()

    @staticmethod
    def _params(row: BatchRow) -> tuple[object, ...]:
        return (
            str(row.id),
            row.source_url,
            row.title,
            int(row.jobs_created),
            row.skipped_entries,
            row.error.code if row.error else None,
            row.error.message if row.error else None,
            _dt(row.created_at),
            _dt(row.updated_at),
            row.thumbnail_url,
        )

    async def get(self, batch_id: UUID) -> BatchRow | None:
        cursor = await self._conn.execute(
            "SELECT * FROM batches WHERE id = ?", (str(batch_id),)
        )
        row = await cursor.fetchone()
        return _batch_from_row(row) if row is not None else None

    async def list_all(self) -> tuple[BatchRow, ...]:
        cursor = await self._conn.execute("SELECT * FROM batches ORDER BY rowid")
        rows = await cursor.fetchall()
        return tuple(_batch_from_row(row) for row in rows)


def _job_queue_from_row(row: aiosqlite.Row) -> JobQueueRow:
    return JobQueueRow(
        job_id=UUID(row["job_id"]),
        kind=row["kind"],
        source_url=row["source_url"],
        caption_prefix=row["caption_prefix"] or "",
        title=row["title"],
        artifact_id=_uuid_or_none(row["artifact_id"]),
        local_path=Path(row["local_path"]) if row["local_path"] else None,
        filename=row["filename"],
        media_type=row["media_type"],
        size_bytes=row["size_bytes"],
        caption=row["caption"],
    )


class JobQueueRepository:
    def __init__(self, conn: aiosqlite.Connection) -> None:
        self._conn = conn

    async def enqueue(self, row: JobQueueRow) -> None:
        sequence = await _next_sequence(self._conn)
        await self._insert(row, sequence)
        await self._conn.commit()

    async def _insert(self, row: JobQueueRow, sequence: int) -> None:
        await self._conn.execute(
            """
            INSERT INTO job_queue (
                sequence, job_id, kind, source_url, caption_prefix, title,
                artifact_id, local_path, filename, media_type, size_bytes, caption
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                sequence,
                str(row.job_id),
                row.kind,
                row.source_url,
                row.caption_prefix,
                row.title,
                _str_or_none(row.artifact_id),
                _str_or_none(row.local_path),
                row.filename,
                row.media_type,
                row.size_bytes,
                row.caption,
            ),
        )

    async def prepend(self, rows: list[JobQueueRow]) -> None:
        """Insert ``rows`` ahead of everything already queued.

        Playlist children must run before work submitted after the playlist
        but before it finished expanding. Sequence numbers only increase, so
        this re-sequences the whole table: delete everything, insert the new
        rows first, then reinsert the survivors in their original order.
        """
        existing = await self.list_all()
        await self._conn.execute("DELETE FROM job_queue")
        for row in rows:
            sequence = await _next_sequence(self._conn)
            await self._insert(row, sequence)
        for row in existing:
            sequence = await _next_sequence(self._conn)
            await self._insert(row, sequence)
        await self._conn.commit()

    async def dequeue(self) -> JobQueueRow | None:
        cursor = await self._conn.execute(
            "SELECT * FROM job_queue ORDER BY sequence ASC LIMIT 1"
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        await self._conn.execute("DELETE FROM job_queue WHERE id = ?", (row["id"],))
        await self._conn.commit()
        return _job_queue_from_row(row)

    async def remove(self, job_id: UUID) -> None:
        await self._conn.execute(
            "DELETE FROM job_queue WHERE job_id = ?", (str(job_id),)
        )
        await self._conn.commit()

    async def list_all(self) -> tuple[JobQueueRow, ...]:
        cursor = await self._conn.execute("SELECT * FROM job_queue ORDER BY sequence ASC")
        rows = await cursor.fetchall()
        return tuple(_job_queue_from_row(row) for row in rows)

    async def peek_sequence(self) -> int | None:
        cursor = await self._conn.execute(
            "SELECT sequence FROM job_queue ORDER BY sequence ASC LIMIT 1"
        )
        row = await cursor.fetchone()
        return row["sequence"] if row is not None else None


def _playlist_queue_from_row(row: aiosqlite.Row) -> PlaylistQueueRow:
    return PlaylistQueueRow(
        batch_id=UUID(row["batch_id"]),
        source_url=row["source_url"],
        offset=row["offset"],
    )


class BatchQueueRepository:
    def __init__(self, conn: aiosqlite.Connection) -> None:
        self._conn = conn

    async def enqueue(self, row: PlaylistQueueRow) -> None:
        sequence = await _next_sequence(self._conn)
        await self._conn.execute(
            "INSERT INTO batch_queue (sequence, batch_id, source_url, offset) "
            "VALUES (?, ?, ?, ?)",
            (sequence, str(row.batch_id), row.source_url, row.offset),
        )
        await self._conn.commit()

    async def dequeue(self) -> PlaylistQueueRow | None:
        cursor = await self._conn.execute(
            "SELECT * FROM batch_queue ORDER BY sequence ASC LIMIT 1"
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        await self._conn.execute("DELETE FROM batch_queue WHERE id = ?", (row["id"],))
        await self._conn.commit()
        return _playlist_queue_from_row(row)

    async def list_all(self) -> tuple[PlaylistQueueRow, ...]:
        cursor = await self._conn.execute(
            "SELECT * FROM batch_queue ORDER BY sequence ASC"
        )
        rows = await cursor.fetchall()
        return tuple(_playlist_queue_from_row(row) for row in rows)

    async def peek_sequence(self) -> int | None:
        cursor = await self._conn.execute(
            "SELECT sequence FROM batch_queue ORDER BY sequence ASC LIMIT 1"
        )
        row = await cursor.fetchone()
        return row["sequence"] if row is not None else None
