import math
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import dotenv_values


_LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})


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
    ytdlp_timeout_seconds: float
    tg_upload_timeout_seconds: float
    shutdown_grace_seconds: float
    log_level: str = "INFO"

    @classmethod
    def from_env(cls, base_dir: Path) -> "Settings":
        resolved_base = base_dir.resolve()
        file_values = {
            key: value
            for key, value in dotenv_values(resolved_base / ".env").items()
            if value is not None
        }
        values = file_values | dict(os.environ)

        session_path = _session_path(
            _path(resolved_base, _value(values, "TG_SESSION_PATH", "./yt2tg.session"))
        )
        _prepare_session_path(session_path)

        return cls(
            api_id=_integer(values, "TG_API_ID", positive=True),
            api_hash=_required(values, "TG_API_HASH"),
            bot_token=_required(values, "TG_BOT_TOKEN"),
            channel_id=_integer(values, "TG_CHANNEL_ID"),
            session_path=session_path,
            cookies_path=_cookies_path(resolved_base, values),
            artifact_root=_artifact_root(resolved_base, values),
            max_artifact_bytes=_integer(
                values, "MAX_ARTIFACT_BYTES", default="2000000000", positive=True
            ),
            ytdlp_timeout_seconds=_seconds(values, "YTDLP_TIMEOUT_SECONDS", "3600"),
            tg_upload_timeout_seconds=_seconds(
                values, "TG_UPLOAD_TIMEOUT_SECONDS", "3600"
            ),
            shutdown_grace_seconds=_seconds(values, "SHUTDOWN_GRACE_SECONDS", "30"),
            log_level=_log_level(values),
        )


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
    default: str | None = None,
    positive: bool = False,
) -> int:
    text = _required(values, key) if default is None else _value(values, key, default)
    try:
        parsed = int(text)
    except ValueError:
        raise ConfigError(f"{key} must be an integer") from None
    if positive and parsed <= 0:
        raise ConfigError(f"{key} must be positive")
    return parsed


def _seconds(values: Mapping[str, str], key: str, default: str) -> float:
    text = _value(values, key, default)
    try:
        parsed = float(text)
    except ValueError:
        raise ConfigError(f"{key} must be a number") from None
    if parsed <= 0 or not math.isfinite(parsed):
        raise ConfigError(f"{key} must be positive and finite")
    return parsed


def _log_level(values: Mapping[str, str]) -> str:
    level = _value(values, "LOG_LEVEL", "INFO").upper()
    if level not in _LOG_LEVELS:
        raise ConfigError(f"LOG_LEVEL must be one of {', '.join(sorted(_LOG_LEVELS))}")
    return level


def _path(base_dir: Path, value: str) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = base_dir / path
    return Path(os.path.abspath(path))


def _session_path(path: Path) -> Path:
    # Telethon appends this suffix itself, so name the real file up front.
    return path if str(path).endswith(".session") else Path(f"{path}.session")


def _prepare_session_path(session_path: Path) -> None:
    """The session holds Telegram credentials, so keep it owner-only."""
    try:
        session_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        session_path.touch(mode=0o600, exist_ok=True)
        session_path.chmod(0o600)
    except OSError:
        raise ConfigError("TG_SESSION_PATH could not be prepared") from None


def _cookies_path(base_dir: Path, values: Mapping[str, str]) -> Path | None:
    value = values.get("YTDLP_COOKIES_PATH", "./yt-cookies.txt").strip()
    if not value:
        return None
    candidate = _path(base_dir, value)
    return candidate if candidate.is_file() else None


def _artifact_root(base_dir: Path, values: Mapping[str, str]) -> Path:
    root = _path(base_dir, _value(values, "ARTIFACT_ROOT", "/tmp/anything2telegram"))
    canonical = root.resolve()
    # clear_orphans() deletes everything under this root, so refuse a shared one.
    if canonical == Path(canonical.anchor) or canonical == base_dir:
        raise ConfigError("ARTIFACT_ROOT points to an unsafe root")
    return root
