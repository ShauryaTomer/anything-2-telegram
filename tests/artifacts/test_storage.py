import asyncio
from pathlib import Path
from uuid import uuid4

import pytest

from anything2telegram.artifacts.storage import ArtifactStorage, ArtifactStorageError
from anything2telegram.domain import StagedArtifact


class Upload:
    def __init__(self, *chunks: bytes) -> None:
        self._chunks = list(chunks)

    async def read(self, _size: int) -> bytes:
        return self._chunks.pop(0) if self._chunks else b""


@pytest.fixture
def storage(tmp_path: Path) -> ArtifactStorage:
    return ArtifactStorage(tmp_path / "artifacts")


def test_root_is_created_owner_only(tmp_path: Path) -> None:
    root = tmp_path / "nested" / "artifacts"
    ArtifactStorage(root)
    assert root.is_dir()
    assert root.stat().st_mode & 0o777 == 0o700


def test_reserve_sanitizes_filename_and_nests_under_job_and_artifact(
    storage: ArtifactStorage,
) -> None:
    job_id, artifact_id = uuid4(), uuid4()
    reservation = storage.reserve(
        job_id, artifact_id, "my clip!.mp4", "video/mp4", "caption"
    )
    assert reservation.filename == "my_clip_.mp4"
    assert reservation.destination == (
        storage.root / str(job_id) / str(artifact_id) / "my_clip_.mp4"
    )
    assert reservation.destination.parent.is_dir()


@pytest.mark.parametrize("filename", ["", "   ", ".", "..", "a" * 300])
def test_reserve_rejects_unusable_filenames(
    storage: ArtifactStorage, filename: str
) -> None:
    with pytest.raises(ArtifactStorageError) as error:
        storage.reserve(uuid4(), uuid4(), filename, None, None)
    assert error.value.code == "invalid_filename"


@pytest.mark.parametrize(
    "filename,expected",
    [("../../etc/passwd", ".._.._etc_passwd"), ("a/b.mp4", "a_b.mp4"), ("///", "_")],
)
def test_reserve_neutralizes_path_separators(
    storage: ArtifactStorage, filename: str, expected: str
) -> None:
    reservation = storage.reserve(uuid4(), uuid4(), filename, None, None)
    assert reservation.filename == expected
    assert reservation.destination.parent == (
        storage.root / str(reservation.job_id) / str(reservation.artifact_id)
    )


def test_reserve_rejects_an_existing_destination(storage: ArtifactStorage) -> None:
    job_id, artifact_id = uuid4(), uuid4()
    reservation = storage.reserve(job_id, artifact_id, "clip.mp4", None, None)
    reservation.destination.write_bytes(b"already here")
    with pytest.raises(ArtifactStorageError) as error:
        storage.reserve(job_id, artifact_id, "clip.mp4", None, None)
    assert error.value.code == "storage_collision"


async def test_stage_writes_the_whole_stream_owner_only(
    storage: ArtifactStorage,
) -> None:
    reservation = storage.reserve(uuid4(), uuid4(), "clip.mp4", "video/mp4", "hi")
    staged = await storage.stage(Upload(b"abc", b"de"), reservation, 1024)
    assert staged.size_bytes == 5
    assert staged.local_path.read_bytes() == b"abcde"
    assert staged.local_path.stat().st_mode & 0o777 == 0o600
    assert (staged.filename, staged.media_type, staged.caption) == (
        "clip.mp4",
        "video/mp4",
        "hi",
    )


async def test_stage_over_the_limit_fails_and_removes_the_job_directory(
    storage: ArtifactStorage,
) -> None:
    job_id = uuid4()
    reservation = storage.reserve(job_id, uuid4(), "clip.mp4", None, None)
    with pytest.raises(ArtifactStorageError) as error:
        await storage.stage(Upload(b"x" * 10), reservation, 4)
    assert error.value.code == "staging_oversize"
    assert not (storage.root / str(job_id)).exists()


async def test_stage_cleans_up_when_reading_the_body_fails(
    storage: ArtifactStorage,
) -> None:
    class Failing:
        async def read(self, _size: int) -> bytes:
            raise ConnectionResetError()

    job_id = uuid4()
    reservation = storage.reserve(job_id, uuid4(), "clip.mp4", None, None)
    with pytest.raises(ArtifactStorageError) as error:
        await storage.stage(Failing(), reservation, 1024)
    assert error.value.code == "staging_io_error"
    assert not (storage.root / str(job_id)).exists()


async def test_stage_propagates_cancellation_after_cleaning_up(
    storage: ArtifactStorage,
) -> None:
    class Cancelling:
        async def read(self, _size: int) -> bytes:
            raise asyncio.CancelledError()

    job_id = uuid4()
    reservation = storage.reserve(job_id, uuid4(), "clip.mp4", None, None)
    with pytest.raises(asyncio.CancelledError):
        await storage.stage(Cancelling(), reservation, 1024)
    assert not (storage.root / str(job_id)).exists()


async def test_validate_accepts_a_staged_artifact_and_rejects_drift(
    storage: ArtifactStorage,
) -> None:
    reservation = storage.reserve(uuid4(), uuid4(), "clip.mp4", None, None)
    staged = await storage.stage(Upload(b"abcde"), reservation, 1024)
    storage.validate_staged_artifact(staged)

    staged.local_path.write_bytes(b"longer than before")
    with pytest.raises(ArtifactStorageError) as error:
        storage.validate_staged_artifact(staged)
    assert error.value.code == "invalid_reservation"


def test_validate_rejects_a_path_outside_its_own_artifact_directory(
    storage: ArtifactStorage, tmp_path: Path
) -> None:
    outside = tmp_path / "elsewhere.mp4"
    outside.write_bytes(b"abc")
    staged = StagedArtifact(uuid4(), uuid4(), outside, "elsewhere.mp4", None, 3, None)
    with pytest.raises(ArtifactStorageError) as error:
        storage.validate_staged_artifact(staged)
    assert error.value.code == "invalid_reservation"


def test_validate_rejects_a_symlink_standing_in_for_the_artifact(
    storage: ArtifactStorage, tmp_path: Path
) -> None:
    target = tmp_path / "target.mp4"
    target.write_bytes(b"abc")
    job_id, artifact_id = uuid4(), uuid4()
    link = storage.allocate_download_directory(job_id, artifact_id) / "clip.mp4"
    link.symlink_to(target)
    staged = StagedArtifact(job_id, artifact_id, link, "clip.mp4", None, 3, None)
    with pytest.raises(ArtifactStorageError) as error:
        storage.validate_staged_artifact(staged)
    assert error.value.code == "invalid_reservation"


def test_download_directory_size_sums_regular_files_only(
    storage: ArtifactStorage,
) -> None:
    job_id, artifact_id = uuid4(), uuid4()
    directory = storage.allocate_download_directory(job_id, artifact_id)
    (directory / "part.mp4").write_bytes(b"x" * 7)
    (directory / "other.m4a").write_bytes(b"y" * 3)
    (directory / "nested").mkdir()
    assert storage.download_directory_size(job_id, artifact_id) == 10


def test_delete_job_directory_is_recursive_and_tolerates_absence(
    storage: ArtifactStorage,
) -> None:
    job_id = uuid4()
    storage.allocate_download_directory(job_id, uuid4()).joinpath("f").write_bytes(b"x")
    storage.delete_job_directory(job_id)
    assert not (storage.root / str(job_id)).exists()
    storage.delete_job_directory(job_id)


def test_clear_orphans_empties_the_root_but_keeps_it(storage: ArtifactStorage) -> None:
    storage.allocate_download_directory(uuid4(), uuid4()).joinpath("f").write_bytes(
        b"x"
    )
    (storage.root / "stray.txt").write_bytes(b"x")
    storage.clear_orphans()
    assert storage.root.is_dir()
    assert list(storage.root.iterdir()) == []
