"""Filesystem staging for artifacts, one directory per job and artifact."""

import errno
import os
import re
import shutil
import stat
from pathlib import Path
from uuid import UUID

from ..domain import StagedArtifact, UploadReservation


_SAFE_FILENAME = re.compile(r"[^A-Za-z0-9._-]+")
_CHUNK_SIZE = 64 * 1024
_DIRECTORY_MODE = 0o700
_FILE_MODE = 0o600

_ERROR_MESSAGES = {
    "invalid_filename": "Artifact filename is invalid",
    "invalid_reservation": "Upload reservation is invalid",
    "staging_disk_full": "Artifact storage is full",
    "staging_io_error": "Artifact could not be stored",
    "staging_oversize": "Upload exceeds maximum allowed size",
    "storage_collision": "Artifact storage path is not safe",
}


class ArtifactStorageError(Exception):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(message)


def _error(code: str) -> ArtifactStorageError:
    return ArtifactStorageError(code, _ERROR_MESSAGES[code])


def _sanitize_filename(filename: str) -> str:
    sanitized = _SAFE_FILENAME.sub("_", filename.strip())
    if sanitized in {"", ".", ".."} or len(sanitized.encode()) > 255:
        raise _error("invalid_filename")
    return sanitized


def _create_private_file(path: str, flags: int) -> int:
    return os.open(path, flags, _FILE_MODE)


class ArtifactStorage:
    def __init__(self, root: Path) -> None:
        self.root = root
        root.mkdir(mode=_DIRECTORY_MODE, parents=True, exist_ok=True)

    def reserve(
        self,
        job_id: UUID,
        artifact_id: UUID,
        filename: str,
        media_type: str | None,
        caption: str | None,
    ) -> UploadReservation:
        safe_filename = _sanitize_filename(filename)
        destination = (
            self.allocate_download_directory(job_id, artifact_id) / safe_filename
        )
        if destination.exists():
            raise _error("storage_collision")
        return UploadReservation(
            job_id,
            artifact_id,
            destination,
            safe_filename,
            media_type,
            caption,
        )

    def allocate_download_directory(self, job_id: UUID, artifact_id: UUID) -> Path:
        directory = self.root / str(job_id) / str(artifact_id)
        directory.mkdir(mode=_DIRECTORY_MODE, parents=True, exist_ok=True)
        return directory

    def download_directory_size(self, job_id: UUID, artifact_id: UUID) -> int:
        directory = self.root / str(job_id) / str(artifact_id)
        return sum(
            entry.stat().st_size for entry in directory.iterdir() if entry.is_file()
        )

    async def stage(
        self,
        upload: object,
        reservation: UploadReservation,
        max_bytes: int,
    ) -> StagedArtifact:
        size_bytes = 0
        try:
            try:
                with open(
                    reservation.destination, "xb", opener=_create_private_file
                ) as sink:
                    while True:
                        chunk = await upload.read(_CHUNK_SIZE)
                        if not chunk:
                            break
                        size_bytes += len(chunk)
                        if size_bytes > max_bytes:
                            raise _error("staging_oversize")
                        sink.write(chunk)
            except FileExistsError:
                raise _error("storage_collision") from None
            except OSError as error:
                if error.errno == errno.ENOSPC:
                    raise _error("staging_disk_full") from None
                raise _error("staging_io_error") from None
        except BaseException:
            self.delete_job_directory(reservation.job_id)
            raise

        return StagedArtifact(
            reservation.job_id,
            reservation.artifact_id,
            reservation.destination,
            reservation.filename,
            reservation.media_type,
            size_bytes,
            reservation.caption,
        )

    def validate_staged_artifact(self, staged: StagedArtifact) -> None:
        expected_parent = self.root / str(staged.job_id) / str(staged.artifact_id)
        if staged.local_path.parent != expected_parent:
            raise _error("invalid_reservation")
        try:
            status = staged.local_path.lstat()
        except OSError:
            raise _error("invalid_reservation") from None
        if not stat.S_ISREG(status.st_mode) or status.st_size != staged.size_bytes:
            raise _error("invalid_reservation")

    def delete_job_directory(self, job_id: UUID) -> None:
        shutil.rmtree(self.root / str(job_id), ignore_errors=True)

    def clear_orphans(self) -> None:
        for entry in self.root.iterdir():
            if entry.is_dir() and not entry.is_symlink():
                shutil.rmtree(entry, ignore_errors=True)
            else:
                entry.unlink(missing_ok=True)
