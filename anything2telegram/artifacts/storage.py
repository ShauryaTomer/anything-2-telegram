import asyncio
import errno
import logging
import os
import re
import shutil
import stat
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import Iterator, Protocol
from uuid import UUID

from starlette.requests import ClientDisconnect

from ..domain import StagedArtifact, UploadReservation


OWNERSHIP_MARKER_NAME = ".anything2telegram-owned"
OWNERSHIP_MARKER_CONTENT = "anything2telegram artifact storage v1\n"
_MARKER_BYTES = OWNERSHIP_MARKER_CONTENT.encode()
_CHUNK_SIZE = 64 * 1024
_SAFE_FILENAME = re.compile(r"[^A-Za-z0-9._-]+")
_DIRECTORY_MODE = 0o700
_FILE_MODE = 0o600
_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
_DIRECTORY = getattr(os, "O_DIRECTORY", 0)
_DIRECTORY_FLAGS = os.O_RDONLY | _NOFOLLOW | _DIRECTORY
_FILE_CREATE_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | _NOFOLLOW
_FILE_READ_FLAGS = os.O_RDONLY | os.O_NONBLOCK | _NOFOLLOW

_ERROR_MESSAGES = {
    "invalid_filename": "Artifact filename is invalid",
    "invalid_identifier": "Artifact identifier is invalid",
    "invalid_limit": "Upload size limit is invalid",
    "invalid_reservation": "Upload reservation is invalid",
    "staging_disk_full": "Artifact storage is full",
    "staging_io_error": "Artifact could not be stored",
    "staging_oversize": "Upload exceeds maximum allowed size",
    "storage_collision": "Artifact storage path is not safe",
    "storage_io_error": "Artifact storage operation failed",
    "storage_unowned": "Artifact storage ownership could not be verified",
}


class AsyncUpload(Protocol):
    async def read(self, size: int) -> bytes: ...


class ArtifactStorageError(Exception):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(message)


def _error(code: str) -> ArtifactStorageError:
    return ArtifactStorageError(code, _ERROR_MESSAGES[code])


class ArtifactStorage:
    def __init__(self, root: Path, logger: object | None = None) -> None:
        if not isinstance(root, Path):
            raise TypeError("root must be Path")
        self.root = root
        self._logger = logger if logger is not None else logging.getLogger(__name__)
        self._require_secure_platform()
        self._root_identity = self._prepare_root()

    def reserve(
        self,
        job_id: UUID,
        artifact_id: UUID,
        filename: str,
        media_type: str | None,
        caption: str | None,
    ) -> UploadReservation:
        self._require_uuid(job_id)
        self._require_uuid(artifact_id)
        safe_filename = self._sanitize_filename(filename)
        job_name = str(job_id)
        artifact_name = str(artifact_id)
        with self._owned_root_fd() as root_fd, ExitStack() as stack:
            job_fd = stack.enter_context(
                self._created_directory_fd(root_fd, job_name)
            )
            artifact_fd = stack.enter_context(
                self._created_directory_fd(job_fd, artifact_name)
            )
            if self._entry_exists(artifact_fd, safe_filename):
                raise _error("storage_collision")
            self._require_directory_tree_unchanged(
                root_fd, job_name, job_fd, artifact_name, artifact_fd
            )

        destination = self.root / job_name / artifact_name / safe_filename
        return UploadReservation(
            job_id,
            artifact_id,
            destination,
            safe_filename,
            media_type,
            caption,
        )

    def allocate_download_directory(
        self, job_id: UUID, artifact_id: UUID
    ) -> Path:
        self._require_uuid(job_id)
        self._require_uuid(artifact_id)
        job_name = str(job_id)
        artifact_name = str(artifact_id)
        with self._owned_root_fd() as root_fd, ExitStack() as stack:
            job_fd = stack.enter_context(
                self._created_directory_fd(root_fd, job_name)
            )
            artifact_fd = stack.enter_context(
                self._created_directory_fd(job_fd, artifact_name)
            )
            self._require_directory_tree_unchanged(
                root_fd, job_name, job_fd, artifact_name, artifact_fd
            )
        return self.root / job_name / artifact_name

    def download_directory_size(self, job_id: UUID, artifact_id: UUID) -> int:
        self._require_uuid(job_id)
        self._require_uuid(artifact_id)
        job_name = str(job_id)
        artifact_name = str(artifact_id)
        total = 0
        with self._owned_root_fd() as root_fd, ExitStack() as stack:
            job_fd = self._stage_directory_fd(root_fd, job_name)
            stack.callback(os.close, job_fd)
            artifact_fd = self._stage_directory_fd(job_fd, artifact_name)
            stack.callback(os.close, artifact_fd)
            try:
                with os.scandir(artifact_fd) as entries:
                    for entry in entries:
                        status = entry.stat(follow_symlinks=False)
                        if stat.S_ISREG(status.st_mode):
                            total += status.st_size
            except OSError:
                raise _error("storage_io_error") from None
            self._require_directory_tree_unchanged(
                root_fd, job_name, job_fd, artifact_name, artifact_fd
            )
        return total

    async def stage(
        self,
        upload: AsyncUpload,
        reservation: UploadReservation,
        max_bytes: int,
    ) -> StagedArtifact:
        self._validate_reservation(reservation)
        if type(max_bytes) is not int or max_bytes < 0:
            raise _error("invalid_limit")

        try:
            size_bytes = await self._write_new_leaf(upload, reservation, max_bytes)
        except (asyncio.CancelledError, ClientDisconnect):
            self._remove_partial_job(reservation.job_id)
            raise
        except ArtifactStorageError:
            self._remove_partial_job(reservation.job_id)
            raise
        except OSError as error:
            self._remove_partial_job(reservation.job_id)
            if error.errno == errno.ENOSPC:
                raise _error("staging_disk_full") from None
            raise _error("staging_io_error") from None
        except Exception:
            self._remove_partial_job(reservation.job_id)
            raise _error("staging_io_error") from None

        return StagedArtifact(
            reservation.job_id,
            reservation.artifact_id,
            reservation.destination,
            reservation.filename,
            reservation.media_type,
            size_bytes,
            reservation.caption,
        )

    def validate_staged_artifact(
        self,
        staged: StagedArtifact,
        expected_identity: tuple[int, int] | None = None,
    ) -> tuple[int, int]:
        if not isinstance(staged, StagedArtifact):
            raise _error("invalid_reservation")
        if expected_identity is not None and (
            not isinstance(expected_identity, tuple)
            or len(expected_identity) != 2
            or not all(type(value) is int for value in expected_identity)
        ):
            raise _error("invalid_reservation")
        reservation = UploadReservation(
            staged.job_id,
            staged.artifact_id,
            staged.local_path,
            staged.filename,
            staged.media_type,
            staged.caption,
        )
        self._validate_reservation(reservation)
        job_name = str(staged.job_id)
        artifact_name = str(staged.artifact_id)
        with self._owned_root_fd() as root_fd, ExitStack() as stack:
            job_fd = self._stage_directory_fd(root_fd, job_name)
            stack.callback(os.close, job_fd)
            artifact_fd = self._stage_directory_fd(job_fd, artifact_name)
            stack.callback(os.close, artifact_fd)
            try:
                leaf_fd = os.open(
                    staged.filename,
                    _FILE_READ_FLAGS,
                    dir_fd=artifact_fd,
                )
            except OSError:
                raise _error("invalid_reservation") from None
            stack.callback(os.close, leaf_fd)
            leaf_stat = self._secure_file_fd(leaf_fd, "invalid_reservation")
            self._require_tree_unchanged(
                root_fd,
                job_name,
                job_fd,
                artifact_name,
                artifact_fd,
                staged.filename,
                leaf_fd,
            )
            identity = (leaf_stat.st_dev, leaf_stat.st_ino)
            if (
                leaf_stat.st_size != staged.size_bytes
                or (
                    expected_identity is not None
                    and identity != expected_identity
                )
            ):
                raise _error("invalid_reservation")
            return identity

    def delete_job_directory(self, job_id: UUID) -> None:
        self._require_uuid(job_id)
        job_name = str(job_id)
        with self._owned_root_fd() as root_fd:
            job_fd = self._open_existing_directory_fd(root_fd, job_name)
            if job_fd is None:
                return
            os.close(job_fd)
            self._rmtree_at(root_fd, job_name)

    def clear_orphans(self) -> None:
        with self._owned_root_fd() as root_fd:
            try:
                with os.scandir(root_fd) as entries:
                    for entry in entries:
                        if not self._is_canonical_uuid(entry.name):
                            continue
                        try:
                            if not entry.is_dir(follow_symlinks=False):
                                continue
                            job_fd = self._open_existing_directory_fd(
                                root_fd, entry.name
                            )
                        except ArtifactStorageError:
                            continue
                        if job_fd is None:
                            continue
                        os.close(job_fd)
                        self._rmtree_at(root_fd, entry.name)
            except ArtifactStorageError:
                raise
            except OSError:
                raise _error("storage_io_error") from None

    async def _write_new_leaf(
        self,
        upload: AsyncUpload,
        reservation: UploadReservation,
        max_bytes: int,
    ) -> int:
        job_name = str(reservation.job_id)
        artifact_name = str(reservation.artifact_id)
        with self._owned_root_fd() as root_fd, ExitStack() as stack:
            job_fd = self._stage_directory_fd(root_fd, job_name)
            stack.callback(os.close, job_fd)
            artifact_fd = self._stage_directory_fd(job_fd, artifact_name)
            stack.callback(os.close, artifact_fd)

            try:
                leaf_fd = os.open(
                    reservation.filename,
                    _FILE_CREATE_FLAGS,
                    _FILE_MODE,
                    dir_fd=artifact_fd,
                )
            except FileExistsError:
                raise _error("storage_collision") from None
            except OSError as error:
                if error.errno in {errno.ELOOP, errno.EISDIR}:
                    raise _error("storage_collision") from None
                raise

            stack.callback(os.close, leaf_fd)
            leaf_stat = self._secure_file_fd(leaf_fd, "storage_collision")
            try:
                self._require_tree_unchanged(
                    root_fd,
                    job_name,
                    job_fd,
                    artifact_name,
                    artifact_fd,
                    reservation.filename,
                    leaf_fd,
                )
                size_bytes = await self._stream_upload(
                    upload, leaf_fd, max_bytes
                )
                self._require_tree_unchanged(
                    root_fd,
                    job_name,
                    job_fd,
                    artifact_name,
                    artifact_fd,
                    reservation.filename,
                    leaf_fd,
                )
                final_stat = os.fstat(leaf_fd)
                if (
                    final_stat.st_dev != leaf_stat.st_dev
                    or final_stat.st_ino != leaf_stat.st_ino
                    or final_stat.st_nlink != 1
                ):
                    raise _error("storage_collision")
                return size_bytes
            except BaseException:
                self._unlink_matching_leaf(
                    artifact_fd, reservation.filename, leaf_stat
                )
                raise

    async def _stream_upload(
        self, upload: AsyncUpload, leaf_fd: int, max_bytes: int
    ) -> int:
        size_bytes = 0
        while True:
            chunk = await upload.read(_CHUNK_SIZE)
            if not chunk:
                return size_bytes
            if not isinstance(chunk, bytes):
                raise OSError(errno.EIO, "invalid upload chunk")
            size_bytes += len(chunk)
            if size_bytes > max_bytes:
                raise _error("staging_oversize")
            view = memoryview(chunk)
            while view:
                written = os.write(leaf_fd, view)
                if written <= 0:
                    raise OSError(errno.EIO, "artifact write failed")
                view = view[written:]

    def _prepare_root(self) -> tuple[int, int]:
        try:
            root_fd = os.open(self.root, _DIRECTORY_FLAGS)
        except FileNotFoundError:
            try:
                self.root.parent.mkdir(parents=True, exist_ok=True)
                os.mkdir(self.root, _DIRECTORY_MODE)
                root_fd = os.open(self.root, _DIRECTORY_FLAGS)
            except FileExistsError:
                try:
                    root_fd = os.open(self.root, _DIRECTORY_FLAGS)
                except OSError:
                    raise _error("storage_unowned") from None
            except OSError:
                raise _error("storage_io_error") from None
        except OSError:
            raise _error("storage_unowned") from None

        try:
            root_stat = self._inspect_directory_fd(root_fd, "storage_unowned")
            identity = (root_stat.st_dev, root_stat.st_ino)
            if self._directory_is_empty(root_fd):
                self._tighten_directory_fd(root_fd, "storage_unowned")
                self._create_marker(root_fd)
            else:
                marker_fd = self._open_valid_marker_fd(root_fd)
                try:
                    self._tighten_directory_fd(root_fd, "storage_unowned")
                    self._tighten_file_fd(marker_fd, "storage_unowned")
                finally:
                    os.close(marker_fd)
            return identity
        finally:
            os.close(root_fd)

    @contextmanager
    def _owned_root_fd(self) -> Iterator[int]:
        try:
            root_fd = os.open(self.root, _DIRECTORY_FLAGS)
        except OSError:
            raise _error("storage_unowned") from None
        try:
            root_stat = self._inspect_directory_fd(root_fd, "storage_unowned")
            if (root_stat.st_dev, root_stat.st_ino) != self._root_identity:
                raise _error("storage_unowned")
            marker_fd = self._open_valid_marker_fd(root_fd)
            try:
                self._tighten_directory_fd(root_fd, "storage_unowned")
                self._tighten_file_fd(marker_fd, "storage_unowned")
                yield root_fd
            finally:
                os.close(marker_fd)
        finally:
            os.close(root_fd)

    @contextmanager
    def _created_directory_fd(
        self, parent_fd: int, name: str
    ) -> Iterator[int]:
        try:
            os.mkdir(name, _DIRECTORY_MODE, dir_fd=parent_fd)
        except FileExistsError:
            pass
        except OSError:
            raise _error("storage_io_error") from None
        directory_fd = self._open_existing_directory_fd(parent_fd, name)
        if directory_fd is None:
            raise _error("storage_collision")
        try:
            yield directory_fd
        finally:
            os.close(directory_fd)

    def _open_existing_directory_fd(
        self, parent_fd: int, name: str
    ) -> int | None:
        try:
            directory_fd = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
        except FileNotFoundError:
            return None
        except OSError:
            raise _error("storage_collision") from None
        try:
            self._secure_directory_fd(directory_fd, "storage_collision")
        except BaseException:
            os.close(directory_fd)
            raise
        return directory_fd

    def _stage_directory_fd(self, parent_fd: int, name: str) -> int:
        try:
            directory_fd = self._open_existing_directory_fd(parent_fd, name)
        except ArtifactStorageError:
            raise _error("invalid_reservation") from None
        if directory_fd is None:
            raise _error("invalid_reservation")
        return directory_fd

    def _inspect_directory_fd(self, directory_fd: int, code: str):
        try:
            current = os.fstat(directory_fd)
            if not stat.S_ISDIR(current.st_mode) or current.st_uid != os.getuid():
                raise _error(code)
            return current
        except ArtifactStorageError:
            raise
        except OSError:
            raise _error(code) from None

    def _tighten_directory_fd(self, directory_fd: int, code: str):
        current = self._inspect_directory_fd(directory_fd, code)
        try:
            if stat.S_IMODE(current.st_mode) != _DIRECTORY_MODE:
                os.fchmod(directory_fd, _DIRECTORY_MODE)
                current = self._inspect_directory_fd(directory_fd, code)
            if stat.S_IMODE(current.st_mode) != _DIRECTORY_MODE:
                raise _error(code)
            return current
        except ArtifactStorageError:
            raise
        except OSError:
            raise _error(code) from None

    def _secure_directory_fd(self, directory_fd: int, code: str):
        self._inspect_directory_fd(directory_fd, code)
        return self._tighten_directory_fd(directory_fd, code)

    def _inspect_file_fd(self, file_fd: int, code: str):
        try:
            current = os.fstat(file_fd)
            if (
                not stat.S_ISREG(current.st_mode)
                or current.st_uid != os.getuid()
                or current.st_nlink != 1
            ):
                raise _error(code)
            return current
        except ArtifactStorageError:
            raise
        except OSError:
            raise _error(code) from None

    def _tighten_file_fd(self, file_fd: int, code: str):
        current = self._inspect_file_fd(file_fd, code)
        try:
            if stat.S_IMODE(current.st_mode) != _FILE_MODE:
                os.fchmod(file_fd, _FILE_MODE)
                current = self._inspect_file_fd(file_fd, code)
            if stat.S_IMODE(current.st_mode) != _FILE_MODE:
                raise _error(code)
            return current
        except ArtifactStorageError:
            raise
        except OSError:
            raise _error(code) from None

    def _secure_file_fd(self, file_fd: int, code: str):
        self._inspect_file_fd(file_fd, code)
        return self._tighten_file_fd(file_fd, code)

    def _create_marker(self, root_fd: int) -> None:
        try:
            marker_fd = os.open(
                OWNERSHIP_MARKER_NAME,
                _FILE_CREATE_FLAGS,
                _FILE_MODE,
                dir_fd=root_fd,
            )
        except FileExistsError:
            self._validate_marker(root_fd)
            return
        except OSError:
            raise _error("storage_unowned") from None
        created_stat = None
        try:
            created_stat = os.fstat(marker_fd)
            self._secure_file_fd(marker_fd, "storage_unowned")
            remaining = memoryview(_MARKER_BYTES)
            while remaining:
                written = os.write(marker_fd, remaining)
                if written <= 0:
                    raise OSError(errno.EIO, "marker write failed")
                remaining = remaining[written:]
            os.fsync(marker_fd)
        except ArtifactStorageError:
            if created_stat is not None:
                self._unlink_matching_leaf(
                    root_fd, OWNERSHIP_MARKER_NAME, created_stat
                )
            raise
        except OSError:
            if created_stat is not None:
                self._unlink_matching_leaf(
                    root_fd, OWNERSHIP_MARKER_NAME, created_stat
                )
            raise _error("storage_io_error") from None
        finally:
            os.close(marker_fd)

    def _validate_marker(self, root_fd: int) -> None:
        marker_fd = self._open_valid_marker_fd(root_fd)
        try:
            self._tighten_file_fd(marker_fd, "storage_unowned")
        finally:
            os.close(marker_fd)

    def _open_valid_marker_fd(self, root_fd: int) -> int:
        try:
            marker_fd = os.open(
                OWNERSHIP_MARKER_NAME,
                os.O_RDONLY | os.O_NONBLOCK | _NOFOLLOW,
                dir_fd=root_fd,
            )
        except OSError:
            raise _error("storage_unowned") from None
        try:
            self._inspect_file_fd(marker_fd, "storage_unowned")
            os.lseek(marker_fd, 0, os.SEEK_SET)
            content = os.read(marker_fd, len(_MARKER_BYTES) + 1)
            if content != _MARKER_BYTES:
                raise _error("storage_unowned")
            return marker_fd
        except BaseException:
            os.close(marker_fd)
            raise

    def _validate_reservation(self, reservation: UploadReservation) -> None:
        if not isinstance(reservation, UploadReservation):
            raise _error("invalid_reservation")
        self._require_uuid(reservation.job_id, "invalid_reservation")
        self._require_uuid(reservation.artifact_id, "invalid_reservation")
        try:
            safe_filename = self._sanitize_filename(reservation.filename)
        except ArtifactStorageError:
            raise _error("invalid_reservation") from None
        expected = (
            self.root
            / str(reservation.job_id)
            / str(reservation.artifact_id)
            / safe_filename
        )
        if reservation.filename != safe_filename or reservation.destination != expected:
            raise _error("invalid_reservation")

    def _require_tree_unchanged(
        self,
        root_fd: int,
        job_name: str,
        job_fd: int,
        artifact_name: str,
        artifact_fd: int,
        filename: str,
        leaf_fd: int,
    ) -> None:
        if (
            not self._root_path_matches(root_fd)
            or not self._entry_matches_fd(root_fd, job_name, job_fd, stat.S_ISDIR)
            or not self._entry_matches_fd(
                job_fd, artifact_name, artifact_fd, stat.S_ISDIR
            )
            or not self._entry_matches_fd(
                artifact_fd, filename, leaf_fd, stat.S_ISREG
            )
        ):
            raise _error("invalid_reservation")

    def _require_directory_tree_unchanged(
        self,
        root_fd: int,
        job_name: str,
        job_fd: int,
        artifact_name: str,
        artifact_fd: int,
    ) -> None:
        if (
            not self._root_path_matches(root_fd)
            or not self._entry_matches_fd(root_fd, job_name, job_fd, stat.S_ISDIR)
            or not self._entry_matches_fd(
                job_fd, artifact_name, artifact_fd, stat.S_ISDIR
            )
        ):
            raise _error("storage_collision")

    def _root_path_matches(self, root_fd: int) -> bool:
        try:
            path_stat = os.stat(self.root, follow_symlinks=False)
            open_stat = os.fstat(root_fd)
        except OSError:
            return False
        return (
            stat.S_ISDIR(path_stat.st_mode)
            and path_stat.st_dev == open_stat.st_dev
            and path_stat.st_ino == open_stat.st_ino
        )

    @staticmethod
    def _entry_matches_fd(
        parent_fd: int,
        name: str,
        opened_fd: int,
        expected_type,
    ) -> bool:
        try:
            entry_stat = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            opened_stat = os.fstat(opened_fd)
        except OSError:
            return False
        return (
            expected_type(entry_stat.st_mode)
            and entry_stat.st_dev == opened_stat.st_dev
            and entry_stat.st_ino == opened_stat.st_ino
        )

    @staticmethod
    def _unlink_matching_leaf(
        artifact_fd: int, filename: str, expected_stat
    ) -> None:
        try:
            current = os.stat(
                filename, dir_fd=artifact_fd, follow_symlinks=False
            )
            if (
                current.st_dev == expected_stat.st_dev
                and current.st_ino == expected_stat.st_ino
            ):
                os.unlink(filename, dir_fd=artifact_fd)
        except OSError:
            pass

    def _remove_partial_job(self, job_id: UUID) -> None:
        try:
            self.delete_job_directory(job_id)
        except Exception:
            try:
                self._logger.error("Artifact staging cleanup failed")
            except Exception:
                pass

    @staticmethod
    def _entry_exists(parent_fd: int, name: str) -> bool:
        try:
            os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            return True
        except FileNotFoundError:
            return False
        except OSError:
            raise _error("storage_io_error") from None

    @staticmethod
    def _directory_is_empty(directory_fd: int) -> bool:
        try:
            with os.scandir(directory_fd) as entries:
                return next(entries, None) is None
        except OSError:
            raise _error("storage_io_error") from None

    @staticmethod
    def _rmtree_at(root_fd: int, name: str) -> None:
        try:
            shutil.rmtree(name, dir_fd=root_fd)
        except FileNotFoundError:
            return
        except OSError:
            raise _error("storage_io_error") from None

    @staticmethod
    def _sanitize_filename(filename: str) -> str:
        if not isinstance(filename, str):
            raise _error("invalid_filename")
        if (
            not filename.strip()
            or filename in {".", ".."}
            or "/" in filename
            or "\\" in filename
        ):
            raise _error("invalid_filename")
        sanitized = _SAFE_FILENAME.sub("_", filename)
        if (
            sanitized in {"", ".", ".."}
            or len(sanitized.encode("utf-8")) > 255
        ):
            raise _error("invalid_filename")
        return sanitized

    @staticmethod
    def _require_uuid(value: object, code: str = "invalid_identifier") -> None:
        if not isinstance(value, UUID):
            raise _error(code)

    @staticmethod
    def _is_canonical_uuid(value: str) -> bool:
        try:
            return str(UUID(value)) == value
        except ValueError:
            return False

    @staticmethod
    def _require_secure_platform() -> None:
        required_dir_fd = {os.open, os.mkdir, os.stat, os.unlink}
        if (
            not _NOFOLLOW
            or not _DIRECTORY
            or not required_dir_fd.issubset(os.supports_dir_fd)
            or os.stat not in os.supports_follow_symlinks
            or os.scandir not in os.supports_fd
            or not getattr(shutil.rmtree, "avoids_symlink_attacks", False)
        ):
            raise _error("storage_unowned")
