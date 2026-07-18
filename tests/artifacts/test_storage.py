import asyncio
import errno
import os
import stat
from pathlib import Path
from unittest.mock import Mock
from uuid import UUID, uuid4

import pytest

from anything2telegram.domain import UploadReservation
from anything2telegram.artifacts import storage as storage_module


def _api():
    from anything2telegram.artifacts.storage import (
        OWNERSHIP_MARKER_CONTENT,
        OWNERSHIP_MARKER_NAME,
        ArtifactStorage,
        ArtifactStorageError,
    )

    return (
        ArtifactStorage,
        ArtifactStorageError,
        OWNERSHIP_MARKER_NAME,
        OWNERSHIP_MARKER_CONTENT,
    )


def _storage(tmp_path: Path):
    ArtifactStorage, _, _, _ = _api()
    return ArtifactStorage(tmp_path / "artifacts")


class ChunkedUpload:
    def __init__(self, chunks: list[bytes | BaseException]) -> None:
        self._chunks = iter(chunks)
        self.read_sizes: list[int] = []

    async def read(self, size: int) -> bytes:
        self.read_sizes.append(size)
        chunk = next(self._chunks, b"")
        if isinstance(chunk, BaseException):
            raise chunk
        return chunk


def test_new_root_gets_ownership_marker(tmp_path: Path) -> None:
    ArtifactStorage, _, marker_name, marker_content = _api()
    root = tmp_path / "new" / "artifacts"

    ArtifactStorage(root)

    assert root.is_dir()
    assert (root / marker_name).read_text() == marker_content


def test_empty_root_gets_ownership_marker(tmp_path: Path) -> None:
    ArtifactStorage, _, marker_name, marker_content = _api()
    root = tmp_path / "artifacts"
    root.mkdir()

    ArtifactStorage(root)

    assert (root / marker_name).read_text() == marker_content


def test_existing_owned_root_and_created_entries_are_private(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o777)
    root.chmod(0o777)
    ArtifactStorage, _, marker_name, _ = _api()

    storage = ArtifactStorage(root)
    artifact_dir = storage.allocate_download_directory(uuid4(), uuid4())

    assert stat.S_IMODE(root.stat().st_mode) == 0o700
    assert stat.S_IMODE((root / marker_name).stat().st_mode) == 0o600
    assert stat.S_IMODE(artifact_dir.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(artifact_dir.stat().st_mode) == 0o700


def test_existing_owned_job_and_artifact_modes_are_tightened(
    tmp_path: Path,
) -> None:
    storage = _storage(tmp_path)
    job_id = uuid4()
    artifact_id = uuid4()
    artifact_dir = storage.root / str(job_id) / str(artifact_id)
    artifact_dir.mkdir(parents=True)
    artifact_dir.parent.chmod(0o777)
    artifact_dir.chmod(0o777)

    storage.allocate_download_directory(job_id, artifact_id)

    assert stat.S_IMODE(artifact_dir.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(artifact_dir.stat().st_mode) == 0o700


def test_hardlinked_ownership_marker_is_rejected(tmp_path: Path) -> None:
    ArtifactStorage, ArtifactStorageError, marker_name, marker_content = _api()
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    outside_marker = tmp_path / "outside-marker"
    outside_marker.write_text(marker_content)
    outside_marker.chmod(0o600)
    os.link(outside_marker, root / marker_name)

    with pytest.raises(ArtifactStorageError) as raised:
        ArtifactStorage(root)

    assert raised.value.code == "storage_unowned"


def test_marker_creation_does_not_follow_racing_symlink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ArtifactStorage, _, marker_name, marker_content = _api()
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    victim = tmp_path / "victim"
    victim.write_text("keep")
    original_write_text = Path.write_text

    def racing_write_text(path: Path, data: str, *args, **kwargs):
        if path.name == marker_name:
            path.symlink_to(victim)
        return original_write_text(path, data, *args, **kwargs)

    monkeypatch.setattr(Path, "write_text", racing_write_text)

    ArtifactStorage(root)

    assert victim.read_text() == "keep"
    assert not (root / marker_name).is_symlink()
    assert (root / marker_name).read_text() == marker_content


def test_nonempty_unowned_root_is_rejected(tmp_path: Path) -> None:
    ArtifactStorage, ArtifactStorageError, _, _ = _api()
    root = tmp_path / "artifacts"
    root.mkdir()
    (root / "keep.txt").write_text("keep")

    with pytest.raises(ArtifactStorageError) as raised:
        ArtifactStorage(root)

    assert raised.value.code == "storage_unowned"
    assert str(root) not in str(raised.value)
    assert (root / "keep.txt").read_text() == "keep"


def test_wrong_marker_is_rejected(tmp_path: Path) -> None:
    ArtifactStorage, ArtifactStorageError, marker_name, _ = _api()
    root = tmp_path / "artifacts"
    root.mkdir()
    (root / marker_name).write_text("wrong")

    with pytest.raises(ArtifactStorageError) as raised:
        ArtifactStorage(root)

    assert raised.value.code == "storage_unowned"


def test_root_symlink_is_rejected(tmp_path: Path) -> None:
    ArtifactStorage, ArtifactStorageError, _, _ = _api()
    target = tmp_path / "target"
    target.mkdir()
    root = tmp_path / "artifacts"
    root.symlink_to(target, target_is_directory=True)

    with pytest.raises(ArtifactStorageError) as raised:
        ArtifactStorage(root)

    assert raised.value.code == "storage_unowned"


def test_reserve_sanitizes_leaf_and_uses_uuid_layout(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    job_id = uuid4()
    artifact_id = uuid4()

    reservation = storage.reserve(
        job_id,
        artifact_id,
        "report final@2.mp4",
        "video/mp4",
        "caption",
    )

    assert reservation.job_id == job_id
    assert reservation.artifact_id == artifact_id
    assert reservation.filename == "report_final_2.mp4"
    assert reservation.media_type == "video/mp4"
    assert reservation.caption == "caption"
    assert reservation.destination == (
        storage.root / str(job_id) / str(artifact_id) / reservation.filename
    )
    assert reservation.destination.parent.is_dir()
    assert reservation.destination.resolve().is_relative_to(storage.root.resolve())


def test_reserve_rejects_existing_regular_destination(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    _, ArtifactStorageError, _, _ = _api()
    job_id = uuid4()
    artifact_id = uuid4()
    reservation = storage.reserve(job_id, artifact_id, "video.mp4", None, None)
    reservation.destination.write_bytes(b"existing")

    with pytest.raises(ArtifactStorageError) as raised:
        storage.reserve(job_id, artifact_id, "video.mp4", None, None)

    assert raised.value.code == "storage_collision"
    assert reservation.destination.read_bytes() == b"existing"


@pytest.mark.parametrize(
    "filename",
    ["", "   ", ".", "..", "/tmp/video.mp4", "../video.mp4", "a/b.mp4", "a\\b.mp4", "C:\\video.mp4"],
)
def test_reserve_rejects_unsafe_filename(tmp_path: Path, filename: str) -> None:
    storage = _storage(tmp_path)
    _, ArtifactStorageError, _, _ = _api()

    with pytest.raises(ArtifactStorageError) as raised:
        storage.reserve(uuid4(), uuid4(), filename, None, None)

    assert raised.value.code == "invalid_filename"


@pytest.mark.parametrize("field", ["job", "artifact"])
def test_reserve_requires_uuid_ids(tmp_path: Path, field: str) -> None:
    storage = _storage(tmp_path)
    _, ArtifactStorageError, _, _ = _api()
    job_id: object = "not-a-uuid" if field == "job" else uuid4()
    artifact_id: object = "not-a-uuid" if field == "artifact" else uuid4()

    with pytest.raises(ArtifactStorageError) as raised:
        storage.reserve(job_id, artifact_id, "video.mp4", None, None)

    assert raised.value.code == "invalid_identifier"


def test_allocate_download_directory_reuses_safe_directories(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    job_id = uuid4()
    artifact_id = uuid4()
    expected = storage.root / str(job_id) / str(artifact_id)
    expected.mkdir(parents=True)

    allocated = storage.allocate_download_directory(job_id, artifact_id)

    assert allocated == expected
    assert allocated.resolve().is_relative_to(storage.root.resolve())


def test_allocate_parent_swap_fails_closed_without_creating_outside(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    storage = _storage(tmp_path)
    _, ArtifactStorageError, _, _ = _api()
    job_id = uuid4()
    artifact_id = uuid4()
    job_dir = storage.root / str(job_id)
    relocated = storage.root / "relocated-job"
    outside_job = tmp_path / "outside-job"
    outside_job.mkdir()
    original_open = storage_module.os.open
    swapped = False

    def racing_open(path, flags, mode=0o777, *, dir_fd=None):
        nonlocal swapped
        if not swapped and path == str(artifact_id) and dir_fd is not None:
            swapped = True
            job_dir.rename(relocated)
            job_dir.symlink_to(outside_job, target_is_directory=True)
        if dir_fd is None:
            return original_open(path, flags, mode)
        return original_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(storage_module.os, "open", racing_open)

    with pytest.raises(ArtifactStorageError) as raised:
        storage.allocate_download_directory(job_id, artifact_id)

    assert raised.value.code == "storage_collision"
    assert swapped
    assert not (outside_job / str(artifact_id)).exists()


@pytest.mark.parametrize("collision_level", ["job", "artifact"])
@pytest.mark.parametrize("kind", ["file", "symlink"])
def test_allocate_download_directory_rejects_collisions(
    tmp_path: Path, collision_level: str, kind: str
) -> None:
    storage = _storage(tmp_path)
    _, ArtifactStorageError, _, _ = _api()
    job_id = uuid4()
    artifact_id = uuid4()
    job_dir = storage.root / str(job_id)
    collision = job_dir if collision_level == "job" else job_dir / str(artifact_id)
    collision.parent.mkdir(parents=True, exist_ok=True)
    if kind == "file":
        collision.write_text("collision")
    else:
        target = tmp_path / f"target-{collision_level}"
        target.mkdir()
        collision.symlink_to(target, target_is_directory=True)

    with pytest.raises(ArtifactStorageError) as raised:
        storage.allocate_download_directory(job_id, artifact_id)

    assert raised.value.code == "storage_collision"


@pytest.mark.asyncio
async def test_stage_streams_chunks_and_preserves_metadata(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    reservation = storage.reserve(
        uuid4(), uuid4(), "video.mp4", "video/mp4", "hello"
    )
    upload = ChunkedUpload([b"abc", b"def", b"ghi"])

    staged = await storage.stage(upload, reservation, max_bytes=9)

    assert len(upload.read_sizes) == 4
    assert all(size > 0 for size in upload.read_sizes)
    assert staged.job_id == reservation.job_id
    assert staged.artifact_id == reservation.artifact_id
    assert staged.local_path == reservation.destination
    assert staged.filename == reservation.filename
    assert staged.media_type == reservation.media_type
    assert staged.caption == reservation.caption
    assert staged.size_bytes == 9
    assert staged.local_path.read_bytes() == b"abcdefghi"


@pytest.mark.asyncio
async def test_stage_oversize_removes_job_directory(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    _, ArtifactStorageError, _, _ = _api()
    reservation = storage.reserve(uuid4(), uuid4(), "video.mp4", None, None)

    with pytest.raises(ArtifactStorageError) as raised:
        await storage.stage(ChunkedUpload([b"abc", b"de"]), reservation, max_bytes=4)

    assert raised.value.code == "staging_oversize"
    assert not hasattr(raised.value, "status_code")
    assert not (storage.root / str(reservation.job_id)).exists()
    assert str(storage.root) not in str(raised.value)


@pytest.mark.asyncio
async def test_stage_rejects_hardlink_without_modifying_outside_victim(
    tmp_path: Path,
) -> None:
    storage = _storage(tmp_path)
    _, ArtifactStorageError, _, _ = _api()
    reservation = storage.reserve(uuid4(), uuid4(), "video.mp4", None, None)
    victim = tmp_path / "victim.mp4"
    victim.write_bytes(b"keep")
    os.link(victim, reservation.destination)

    with pytest.raises(ArtifactStorageError) as raised:
        await storage.stage(ChunkedUpload([b"overwrite"]), reservation, max_bytes=100)

    assert raised.value.code == "storage_collision"
    assert victim.read_bytes() == b"keep"


@pytest.mark.asyncio
async def test_stage_parent_swap_never_writes_outside_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    storage = _storage(tmp_path)
    reservation = storage.reserve(uuid4(), uuid4(), "video.mp4", None, None)
    job_dir = storage.root / str(reservation.job_id)
    relocated = storage.root / "relocated-job"
    outside_job = tmp_path / "outside-job"
    outside_artifact = outside_job / str(reservation.artifact_id)
    outside_artifact.mkdir(parents=True)
    outside_leaf = outside_artifact / reservation.filename
    original_open = storage_module.os.open
    swapped = False

    def racing_open(path, flags, mode=0o777, *, dir_fd=None):
        nonlocal swapped
        if not swapped and Path(path).name == reservation.filename and flags & os.O_CREAT:
            swapped = True
            job_dir.rename(relocated)
            job_dir.symlink_to(outside_job, target_is_directory=True)
        if dir_fd is None:
            return original_open(path, flags, mode)
        return original_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(storage_module.os, "open", racing_open)

    try:
        await storage.stage(ChunkedUpload([b"data"]), reservation, max_bytes=100)
    except storage_module.ArtifactStorageError:
        pass

    assert swapped
    assert not outside_leaf.exists()


@pytest.mark.asyncio
async def test_stage_cancellation_removes_job_directory_and_reraises(
    tmp_path: Path,
) -> None:
    storage = _storage(tmp_path)
    reservation = storage.reserve(uuid4(), uuid4(), "video.mp4", None, None)
    upload = ChunkedUpload([b"partial", asyncio.CancelledError()])

    with pytest.raises(asyncio.CancelledError):
        await storage.stage(upload, reservation, max_bytes=100)

    assert not (storage.root / str(reservation.job_id)).exists()


@pytest.mark.asyncio
async def test_stage_cleanup_failure_preserves_cancellation_and_logs_safely(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ArtifactStorage, _, _, _ = _api()
    logger = Mock()
    storage = ArtifactStorage(tmp_path / "artifacts", logger=logger)
    reservation = storage.reserve(uuid4(), uuid4(), "video.mp4", None, None)
    monkeypatch.setattr(
        storage,
        "delete_job_directory",
        Mock(side_effect=RuntimeError("/secret/path")),
    )

    with pytest.raises(asyncio.CancelledError):
        await storage.stage(
            ChunkedUpload([b"partial", asyncio.CancelledError()]),
            reservation,
            max_bytes=100,
        )

    logger.error.assert_called_once()
    rendered = " ".join(
        str(value)
        for value in (*logger.error.call_args.args, *logger.error.call_args.kwargs.values())
    )
    assert "/secret" not in rendered
    assert str(reservation.job_id) not in rendered
    assert str(storage.root) not in rendered


@pytest.mark.asyncio
async def test_stage_cleanup_failure_preserves_oversize_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ArtifactStorage, ArtifactStorageError, _, _ = _api()
    logger = Mock()
    storage = ArtifactStorage(tmp_path / "artifacts", logger=logger)
    reservation = storage.reserve(uuid4(), uuid4(), "video.mp4", None, None)
    monkeypatch.setattr(
        storage,
        "delete_job_directory",
        Mock(side_effect=RuntimeError("/secret/path")),
    )

    with pytest.raises(ArtifactStorageError) as raised:
        await storage.stage(ChunkedUpload([b"too-large"]), reservation, max_bytes=1)

    assert raised.value.code == "staging_oversize"
    logger.error.assert_called_once_with("Artifact staging cleanup failed")


@pytest.mark.asyncio
async def test_stage_enospc_maps_safe_error_and_removes_job(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    _, ArtifactStorageError, _, _ = _api()
    reservation = storage.reserve(uuid4(), uuid4(), "video.mp4", None, None)
    upload = ChunkedUpload([b"partial", OSError(errno.ENOSPC, "secret path")])

    with pytest.raises(ArtifactStorageError) as raised:
        await storage.stage(upload, reservation, max_bytes=100)

    assert raised.value.code == "staging_disk_full"
    assert "secret" not in str(raised.value)
    assert not (storage.root / str(reservation.job_id)).exists()


@pytest.mark.asyncio
async def test_stage_generic_io_maps_safe_error_and_removes_job(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    _, ArtifactStorageError, _, _ = _api()
    reservation = storage.reserve(uuid4(), uuid4(), "video.mp4", None, None)
    upload = ChunkedUpload([b"partial", OSError(errno.EIO, "secret path")])

    with pytest.raises(ArtifactStorageError) as raised:
        await storage.stage(upload, reservation, max_bytes=100)

    assert raised.value.code == "staging_io_error"
    assert "secret" not in str(raised.value)
    assert not (storage.root / str(reservation.job_id)).exists()


@pytest.mark.asyncio
async def test_stage_rejects_forged_reservation_outside_root(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    _, ArtifactStorageError, _, _ = _api()
    reservation = UploadReservation(
        uuid4(),
        uuid4(),
        tmp_path / "outside.mp4",
        "outside.mp4",
        None,
        None,
    )

    with pytest.raises(ArtifactStorageError) as raised:
        await storage.stage(ChunkedUpload([b"data"]), reservation, max_bytes=100)

    assert raised.value.code == "invalid_reservation"
    assert not (tmp_path / "outside.mp4").exists()


@pytest.mark.asyncio
async def test_stage_rejects_job_directory_replaced_by_symlink(
    tmp_path: Path,
) -> None:
    storage = _storage(tmp_path)
    _, ArtifactStorageError, _, _ = _api()
    reservation = storage.reserve(uuid4(), uuid4(), "video.mp4", None, None)
    job_dir = storage.root / str(reservation.job_id)
    relocated = storage.root / "relocated"
    job_dir.rename(relocated)
    job_dir.symlink_to(relocated, target_is_directory=True)

    with pytest.raises(ArtifactStorageError) as raised:
        await storage.stage(ChunkedUpload([b"data"]), reservation, max_bytes=100)

    assert raised.value.code == "invalid_reservation"
    assert not reservation.destination.exists()


@pytest.mark.asyncio
async def test_stage_requires_reservation_type(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    _, ArtifactStorageError, _, _ = _api()

    with pytest.raises(ArtifactStorageError) as raised:
        await storage.stage(ChunkedUpload([b"data"]), object(), max_bytes=100)

    assert raised.value.code == "invalid_reservation"


def test_delete_job_directory_is_idempotent(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    job_id = uuid4()
    path = storage.allocate_download_directory(job_id, uuid4())
    (path / "video.mp4").write_bytes(b"data")

    storage.delete_job_directory(job_id)
    storage.delete_job_directory(job_id)

    assert not (storage.root / str(job_id)).exists()


def test_delete_job_directory_rejects_symlink(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    _, ArtifactStorageError, _, _ = _api()
    job_id = uuid4()
    target = tmp_path / "outside"
    target.mkdir()
    (target / "keep.txt").write_text("keep")
    (storage.root / str(job_id)).symlink_to(target, target_is_directory=True)

    with pytest.raises(ArtifactStorageError) as raised:
        storage.delete_job_directory(job_id)

    assert raised.value.code == "storage_collision"
    assert (target / "keep.txt").read_text() == "keep"


def test_delete_job_parent_swap_never_deletes_outside_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    storage = _storage(tmp_path)
    job_id = uuid4()
    artifact_dir = storage.allocate_download_directory(job_id, uuid4())
    (artifact_dir / "delete.txt").write_text("delete")
    outside_root = tmp_path / "outside-root"
    outside_job = outside_root / str(job_id)
    outside_job.mkdir(parents=True)
    victim = outside_job / "keep.txt"
    victim.write_text("keep")
    relocated_root = tmp_path / "relocated-root"
    original_rmtree = storage_module.shutil.rmtree
    swapped = False

    def racing_rmtree(path, *args, **kwargs):
        nonlocal swapped
        if not swapped:
            swapped = True
            storage.root.rename(relocated_root)
            storage.root.symlink_to(outside_root, target_is_directory=True)
        return original_rmtree(path, *args, **kwargs)

    monkeypatch.setattr(storage_module.shutil, "rmtree", racing_rmtree)

    storage.delete_job_directory(job_id)

    assert swapped
    assert victim.read_text() == "keep"


def test_clear_orphans_only_deletes_owned_uuid_directories(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    _, _, marker_name, marker_content = _api()
    orphan = storage.root / str(uuid4())
    orphan.mkdir()
    (orphan / "artifact").write_text("delete")
    unrelated = storage.root / "keep-dir"
    unrelated.mkdir()
    uuid_file = storage.root / str(uuid4())
    uuid_file.write_text("keep")
    target = tmp_path / "symlink-target"
    target.mkdir()
    symlink = storage.root / str(uuid4())
    symlink.symlink_to(target, target_is_directory=True)

    storage.clear_orphans()

    assert not orphan.exists()
    assert unrelated.is_dir()
    assert uuid_file.read_text() == "keep"
    assert symlink.is_symlink()
    assert (storage.root / marker_name).read_text() == marker_content


def test_clear_orphans_fails_when_marker_becomes_invalid(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    _, ArtifactStorageError, marker_name, _ = _api()
    orphan = storage.root / str(uuid4())
    orphan.mkdir()
    (storage.root / marker_name).write_text("wrong")

    with pytest.raises(ArtifactStorageError) as raised:
        storage.clear_orphans()

    assert raised.value.code == "storage_unowned"
    assert orphan.is_dir()


def test_clear_orphans_does_not_use_path_iterdir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    storage = _storage(tmp_path)
    orphan = storage.root / str(uuid4())
    orphan.mkdir(mode=0o700)

    def forbidden_iterdir(path: Path):
        raise AssertionError("pathname iteration is unsafe")

    monkeypatch.setattr(Path, "iterdir", forbidden_iterdir)

    storage.clear_orphans()

    assert not orphan.exists()
