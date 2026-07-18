import asyncio
import errno
import os
import re
import shutil
from pathlib import Path
from typing import Protocol
from uuid import UUID

from starlette.requests import ClientDisconnect

from ..domain import StagedArtifact, UploadReservation


OWNERSHIP_MARKER_NAME = ".anything2telegram-owned"
OWNERSHIP_MARKER_CONTENT = "anything2telegram artifact storage v1\n"
_CHUNK_SIZE = 64 * 1024
_SAFE_FILENAME = re.compile(r"[^A-Za-z0-9._-]+")


class AsyncUpload(Protocol):
    async def read(self, size: int) -> bytes: ...


class ArtifactStorageError(Exception):
    def __init__(self, code: str, message: str, status_code: int = 500) -> None:
        self.code = code
        self.message = message
        self.status_code = status_code
        super().__init__(message)


class ArtifactStorage:
    def __init__(self, root: Path) -> None:
        if not isinstance(root, Path):
            raise TypeError("root must be Path")
        self.root = root
        self._prepare_root()

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
        artifact_dir = self.allocate_download_directory(job_id, artifact_id)
        destination = artifact_dir / safe_filename
        if destination.is_symlink() or (
            destination.exists() and not destination.is_file()
        ):
            raise self._collision_error()
        self._require_contained(destination)
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
        self._validate_owned_root()
        self._require_uuid(job_id)
        self._require_uuid(artifact_id)
        job_dir = self.root / str(job_id)
        artifact_dir = job_dir / str(artifact_id)
        self._ensure_directory(job_dir)
        self._ensure_directory(artifact_dir)
        self._require_contained(artifact_dir)
        return artifact_dir

    async def stage(
        self,
        upload: AsyncUpload,
        reservation: UploadReservation,
        max_bytes: int,
    ) -> StagedArtifact:
        self._validate_owned_root()
        self._validate_reservation(reservation)
        if type(max_bytes) is not int or max_bytes < 0:
            raise ArtifactStorageError(
                "invalid_limit", "Upload size limit is invalid", 500
            )

        size_bytes = 0
        try:
            flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
            flags |= getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(reservation.destination, flags, 0o600)
            with os.fdopen(descriptor, "wb") as destination:
                while True:
                    chunk = await upload.read(_CHUNK_SIZE)
                    if not chunk:
                        break
                    if not isinstance(chunk, bytes):
                        raise OSError(errno.EIO, "invalid upload chunk")
                    size_bytes += len(chunk)
                    if size_bytes > max_bytes:
                        raise ArtifactStorageError(
                            "staging_oversize",
                            "Upload exceeds maximum allowed size",
                            413,
                        )
                    destination.write(chunk)
        except (asyncio.CancelledError, ClientDisconnect):
            self._remove_partial_job(reservation.job_id)
            raise
        except ArtifactStorageError:
            self._remove_partial_job(reservation.job_id)
            raise
        except OSError as error:
            self._remove_partial_job(reservation.job_id)
            if error.errno == errno.ENOSPC:
                raise ArtifactStorageError(
                    "staging_disk_full", "Artifact storage is full", 507
                ) from None
            raise ArtifactStorageError(
                "staging_io_error", "Artifact could not be stored", 500
            ) from None
        except Exception:
            self._remove_partial_job(reservation.job_id)
            raise ArtifactStorageError(
                "staging_io_error", "Artifact could not be stored", 500
            ) from None

        return StagedArtifact(
            reservation.job_id,
            reservation.artifact_id,
            reservation.destination,
            reservation.filename,
            reservation.media_type,
            size_bytes,
            reservation.caption,
        )

    def delete_job_directory(self, job_id: UUID) -> None:
        self._validate_owned_root()
        self._require_uuid(job_id)
        job_dir = self.root / str(job_id)
        if job_dir.is_symlink():
            raise self._collision_error()
        self._require_contained(job_dir)
        if not job_dir.exists():
            return
        if not job_dir.is_dir():
            raise self._collision_error()
        try:
            shutil.rmtree(job_dir)
        except OSError:
            raise ArtifactStorageError(
                "storage_io_error", "Artifact storage operation failed"
            ) from None

    def clear_orphans(self) -> None:
        self._validate_owned_root()
        try:
            entries = tuple(self.root.iterdir())
        except OSError:
            raise ArtifactStorageError(
                "storage_io_error", "Artifact storage operation failed"
            ) from None
        for entry in entries:
            if entry.is_symlink() or not entry.is_dir():
                continue
            if not self._is_canonical_uuid(entry.name):
                continue
            try:
                shutil.rmtree(entry)
            except OSError:
                raise ArtifactStorageError(
                    "storage_io_error", "Artifact storage operation failed"
                ) from None

    def _prepare_root(self) -> None:
        if self.root.is_symlink():
            raise self._unowned_error()
        if self.root.exists():
            if not self.root.is_dir():
                raise self._unowned_error()
        else:
            try:
                self.root.mkdir(parents=True)
            except OSError:
                raise ArtifactStorageError(
                    "storage_io_error", "Artifact storage operation failed"
                ) from None

        marker = self.root / OWNERSHIP_MARKER_NAME
        try:
            entries = tuple(self.root.iterdir())
        except OSError:
            raise ArtifactStorageError(
                "storage_io_error", "Artifact storage operation failed"
            ) from None
        if not entries:
            try:
                marker.write_text(OWNERSHIP_MARKER_CONTENT)
            except OSError:
                raise ArtifactStorageError(
                    "storage_io_error", "Artifact storage operation failed"
                ) from None
            return
        self._validate_owned_root()

    def _validate_owned_root(self) -> None:
        if self.root.is_symlink() or not self.root.is_dir():
            raise self._unowned_error()
        marker = self.root / OWNERSHIP_MARKER_NAME
        if marker.is_symlink() or not marker.is_file():
            raise self._unowned_error()
        try:
            content = marker.read_text()
        except (OSError, UnicodeError):
            raise self._unowned_error() from None
        if content != OWNERSHIP_MARKER_CONTENT:
            raise self._unowned_error()

    def _validate_reservation(self, reservation: UploadReservation) -> None:
        if not isinstance(reservation, UploadReservation):
            raise ArtifactStorageError(
                "invalid_reservation", "Upload reservation is invalid"
            )
        self._require_uuid(reservation.job_id, "invalid_reservation")
        self._require_uuid(reservation.artifact_id, "invalid_reservation")
        try:
            safe_filename = self._sanitize_filename(reservation.filename)
        except ArtifactStorageError:
            raise ArtifactStorageError(
                "invalid_reservation", "Upload reservation is invalid"
            ) from None
        expected_parent = (
            self.root / str(reservation.job_id) / str(reservation.artifact_id)
        )
        expected_job = self.root / str(reservation.job_id)
        expected = expected_parent / safe_filename
        if reservation.filename != safe_filename or reservation.destination != expected:
            raise ArtifactStorageError(
                "invalid_reservation", "Upload reservation is invalid"
            )
        if (
            expected_job.is_symlink()
            or not expected_job.is_dir()
            or expected_parent.is_symlink()
            or not expected_parent.is_dir()
            or expected.parent.resolve() != expected_parent.resolve()
            or reservation.destination.is_symlink()
            or (
                reservation.destination.exists()
                and not reservation.destination.is_file()
            )
        ):
            raise ArtifactStorageError(
                "invalid_reservation", "Upload reservation is invalid"
            )
        self._require_contained(expected)

    def _ensure_directory(self, directory: Path) -> None:
        if directory.is_symlink():
            raise self._collision_error()
        if directory.exists():
            if not directory.is_dir():
                raise self._collision_error()
            return
        try:
            directory.mkdir()
        except FileExistsError:
            if directory.is_symlink() or not directory.is_dir():
                raise self._collision_error() from None
        except OSError:
            raise ArtifactStorageError(
                "storage_io_error", "Artifact storage operation failed"
            ) from None

    def _remove_partial_job(self, job_id: UUID) -> None:
        try:
            self.delete_job_directory(job_id)
        except ArtifactStorageError:
            pass

    def _require_contained(self, path: Path) -> None:
        try:
            contained = path.resolve().is_relative_to(self.root.resolve())
        except OSError:
            contained = False
        if not contained:
            raise ArtifactStorageError(
                "invalid_reservation", "Upload reservation is invalid"
            )

    @staticmethod
    def _sanitize_filename(filename: str) -> str:
        if not isinstance(filename, str):
            raise ArtifactStorageError(
                "invalid_filename", "Artifact filename is invalid", 400
            )
        if (
            not filename.strip()
            or filename in {".", ".."}
            or "/" in filename
            or "\\" in filename
        ):
            raise ArtifactStorageError(
                "invalid_filename", "Artifact filename is invalid", 400
            )
        sanitized = _SAFE_FILENAME.sub("_", filename)
        if sanitized in {"", ".", ".."}:
            raise ArtifactStorageError(
                "invalid_filename", "Artifact filename is invalid", 400
            )
        return sanitized

    @staticmethod
    def _require_uuid(value: object, code: str = "invalid_identifier") -> None:
        if not isinstance(value, UUID):
            raise ArtifactStorageError(code, "Artifact identifier is invalid", 400)

    @staticmethod
    def _is_canonical_uuid(value: str) -> bool:
        try:
            return str(UUID(value)) == value
        except ValueError:
            return False

    @staticmethod
    def _unowned_error() -> ArtifactStorageError:
        return ArtifactStorageError(
            "storage_unowned", "Artifact storage ownership could not be verified"
        )

    @staticmethod
    def _collision_error() -> ArtifactStorageError:
        return ArtifactStorageError(
            "storage_collision", "Artifact storage path is not safe"
        )
