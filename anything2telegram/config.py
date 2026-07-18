import math
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from dotenv import dotenv_values


class ConfigError(ValueError):
    """Configuration is missing, invalid, or unsafe."""


@dataclass(frozen=True)
class Settings:
    api_id: int
    api_hash: str
    bot_token: str
    channel_id: int
    session_path: Path
    cookies_path: Path | None
    artifact_root: Path
    max_artifact_bytes: int
    ytdlp_timeout_seconds: float | int
    tg_upload_timeout_seconds: float | int
    shutdown_grace_seconds: float | int

    def __post_init__(self) -> None:
        if self.max_artifact_bytes <= 0:
            raise ConfigError("MAX_ARTIFACT_BYTES must be positive")
        for key, value in (
            ("YTDLP_TIMEOUT_SECONDS", self.ytdlp_timeout_seconds),
            ("TG_UPLOAD_TIMEOUT_SECONDS", self.tg_upload_timeout_seconds),
            ("SHUTDOWN_GRACE_SECONDS", self.shutdown_grace_seconds),
        ):
            if value <= 0 or not math.isfinite(value):
                raise ConfigError(f"{key} must be positive and finite")

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
        session_path.parent.mkdir(parents=True, exist_ok=True)

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
        if (
            artifact_root == Path(artifact_root.anchor)
            or artifact_root.resolve() == resolved_base
        ):
            raise ConfigError("ARTIFACT_ROOT points to an unsafe root")

        return cls(
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
