import math
import os
import stat
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import dotenv_values


class ConfigError(ValueError):
    """Configuration is missing, invalid, or unsafe."""


@dataclass(frozen=True)
class Settings:
    api_id: int
    api_hash: str = field(repr=False)
    bot_token: str = field(repr=False)
    channel_id: int
    session_path: Path
    cookies_path: Path | None
    artifact_root: Path
    max_artifact_bytes: int
    ytdlp_timeout_seconds: float | int
    tg_upload_timeout_seconds: float | int
    shutdown_grace_seconds: float | int

    def __post_init__(self) -> None:
        _validate_int(self.api_id, "TG_API_ID", positive=True)
        _validate_nonblank(self.api_hash, "TG_API_HASH")
        _validate_nonblank(self.bot_token, "TG_BOT_TOKEN")
        _validate_int(self.channel_id, "TG_CHANNEL_ID")
        _validate_path(self.session_path, "TG_SESSION_PATH")
        if self.cookies_path is not None:
            _validate_path(self.cookies_path, "YTDLP_COOKIES_PATH")
        _validate_path(self.artifact_root, "ARTIFACT_ROOT")
        _validate_int(
            self.max_artifact_bytes,
            "MAX_ARTIFACT_BYTES",
            positive=True,
        )
        for key, value in (
            ("YTDLP_TIMEOUT_SECONDS", self.ytdlp_timeout_seconds),
            ("TG_UPLOAD_TIMEOUT_SECONDS", self.tg_upload_timeout_seconds),
            ("SHUTDOWN_GRACE_SECONDS", self.shutdown_grace_seconds),
        ):
            _validate_timeout(value, key)

    @classmethod
    def from_env(
        cls,
        base_dir: Path,
        environ: Mapping[str, str] | None = None,
    ) -> "Settings":
        resolved_base = base_dir.resolve()
        file_values = {
            key: value
            for key, value in dotenv_values(resolved_base / ".env").items()
            if value is not None
        }
        environment_values = dict(os.environ if environ is None else environ)
        values = file_values | environment_values

        api_id = _integer(values, "TG_API_ID", required=True)
        api_hash = _required(values, "TG_API_HASH")
        bot_token = _required(values, "TG_BOT_TOKEN")
        channel_id = _integer(values, "TG_CHANNEL_ID", required=True)

        session_path = _path_from_base(
            resolved_base,
            _value(values, "TG_SESSION_PATH", "./yt2tg.session"),
            "TG_SESSION_PATH",
        )
        session_path = _telethon_session_path(session_path)
        cookies_value = values.get("YTDLP_COOKIES_PATH", "./yt-cookies.txt")
        cookies_path = None
        if cookies_value and cookies_value.strip():
            candidate = _path_from_base(
                resolved_base, cookies_value, "YTDLP_COOKIES_PATH"
            )
            if candidate.is_file():
                cookies_path = candidate

        artifact_root = _path_from_base(
            resolved_base,
            _value(values, "ARTIFACT_ROOT", "/tmp/anything2telegram"),
            "ARTIFACT_ROOT",
        )
        canonical_artifact_root = artifact_root.resolve()
        if (
            canonical_artifact_root == Path(canonical_artifact_root.anchor)
            or canonical_artifact_root == resolved_base
        ):
            raise ConfigError("ARTIFACT_ROOT points to an unsafe root")

        settings = cls(
            api_id=api_id,
            api_hash=api_hash,
            bot_token=bot_token,
            channel_id=channel_id,
            session_path=session_path,
            cookies_path=cookies_path,
            artifact_root=artifact_root,
            max_artifact_bytes=_integer(
                values,
                "MAX_ARTIFACT_BYTES",
                default="2000000000",
            ),
            ytdlp_timeout_seconds=_number(
                values,
                "YTDLP_TIMEOUT_SECONDS",
                default="3600",
            ),
            tg_upload_timeout_seconds=_number(
                values,
                "TG_UPLOAD_TIMEOUT_SECONDS",
                default="3600",
            ),
            shutdown_grace_seconds=_number(
                values,
                "SHUTDOWN_GRACE_SECONDS",
                default="30",
            ),
        )
        _prepare_session_path(settings.session_path)
        return settings


def _required(values: Mapping[str, str], key: str) -> str:
    value = values.get(key)
    if value is None or not value.strip():
        raise ConfigError(f"{key} is required")
    return value.strip()


def _value(values: Mapping[str, str], key: str, default: str) -> str:
    value = values.get(key, default)
    if not value.strip():
        raise ConfigError(f"{key} must not be blank")
    return value.strip()


def _integer(
    values: Mapping[str, str],
    key: str,
    *,
    required: bool = False,
    default: str | None = None,
) -> int:
    text = _required(values, key) if required else _value(values, key, default or "")
    try:
        return int(text)
    except ValueError:
        raise ConfigError(f"{key} must be an integer") from None


def _number(values: Mapping[str, str], key: str, *, default: str) -> float | int:
    text = _value(values, key, default)
    try:
        parsed = float(text)
    except ValueError:
        raise ConfigError(f"{key} must be a number") from None
    if parsed.is_integer():
        return int(parsed)
    return parsed


def _path_from_base(base_dir: Path, value: str, key: str) -> Path:
    if not value.strip():
        raise ConfigError(f"{key} must not be blank")
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = base_dir / path
    return Path(os.path.abspath(path))


def _telethon_session_path(path: Path) -> Path:
    if str(path).endswith(".session"):
        return path
    return Path(f"{path}.session")


def _prepare_session_path(session_path: Path) -> None:
    parent = session_path.parent
    descriptor: int | None = None
    try:
        try:
            parent_status = parent.lstat()
        except FileNotFoundError:
            parent_status = None

        if parent_status is None:
            parent.mkdir(mode=0o700, parents=True, exist_ok=False)
            parent.chmod(0o700)
        else:
            if not stat.S_ISDIR(parent_status.st_mode):
                raise OSError
            if stat.S_IMODE(parent_status.st_mode) & 0o022:
                raise OSError

        try:
            session_status = session_path.lstat()
        except FileNotFoundError:
            session_status = None

        if session_status is not None and not stat.S_ISREG(session_status.st_mode):
            raise OSError

        flags = os.O_CREAT | os.O_RDWR
        if session_status is None:
            flags |= os.O_EXCL
        flags |= getattr(os, "O_NOFOLLOW", 0)
        flags |= getattr(os, "O_CLOEXEC", 0)
        descriptor = os.open(session_path, flags, 0o600)
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise OSError
        os.fchmod(descriptor, 0o600)
    except OSError:
        raise ConfigError("TG_SESSION_PATH could not be prepared") from None
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _validate_int(value: object, key: str, *, positive: bool = False) -> None:
    if type(value) is not int:
        raise ConfigError(f"{key} must be an integer")
    if positive and value <= 0:
        raise ConfigError(f"{key} must be positive")


def _validate_nonblank(value: object, key: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{key} must be a nonblank string")


def _validate_path(value: object, key: str) -> None:
    if not isinstance(value, Path):
        raise ConfigError(f"{key} must be a Path")


def _validate_timeout(value: object, key: str) -> None:
    if type(value) not in (int, float):
        raise ConfigError(f"{key} must be a number")
    if value <= 0 or not math.isfinite(value):
        raise ConfigError(f"{key} must be positive and finite")
