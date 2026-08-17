import os
from pathlib import Path

import pytest

from anything2telegram import config
from anything2telegram.config import ConfigError, Settings


REQUIRED = {
    "TG_API_ID": "12345",
    "TG_API_HASH": "hash",
    "TG_BOT_TOKEN": "token",
    "TG_CHANNEL_ID": "-1001234567890",
}


@pytest.fixture
def base_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """An empty project directory with no ambient environment leaking in."""
    for key in list(os.environ):
        if key.startswith(("TG_", "YTDLP_", "ARTIFACT_", "MAX_", "SHUTDOWN_")):
            monkeypatch.delenv(key, raising=False)
    for key, value in REQUIRED.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("ARTIFACT_ROOT", str(tmp_path / "artifacts"))
    # The real browser profiles of whoever runs the suite are not an input.
    monkeypatch.setattr(config, "find_cookie_profile", lambda _name: None)
    return tmp_path


def test_defaults_are_applied_and_paths_are_absolute(base_dir: Path) -> None:
    settings = Settings.from_env(base_dir)

    assert settings.api_id == 12345
    assert settings.channel_id == -1001234567890
    assert settings.max_artifact_bytes == 2000000000
    assert settings.ytdlp_timeout_seconds == 3600
    assert settings.tg_upload_timeout_seconds == 3600
    assert settings.shutdown_grace_seconds == 30
    assert settings.session_path == base_dir / "yt2tg.session"
    assert settings.artifact_root.is_absolute()


def test_dotenv_supplies_values_but_the_environment_wins(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / ".env").write_text(
        "TG_API_ID=1\nTG_API_HASH=from_file\nTG_BOT_TOKEN=from_file\n"
        "TG_CHANNEL_ID=-100\nARTIFACT_ROOT=./artifacts\n"
    )
    monkeypatch.setenv("TG_API_HASH", "from_env")

    settings = Settings.from_env(tmp_path)

    assert settings.api_hash == "from_env"
    assert settings.bot_token == "from_file"


@pytest.mark.parametrize("key", sorted(REQUIRED))
def test_missing_required_values_are_rejected(
    base_dir: Path, monkeypatch: pytest.MonkeyPatch, key: str
) -> None:
    monkeypatch.delenv(key)
    with pytest.raises(ConfigError, match=key):
        Settings.from_env(base_dir)


@pytest.mark.parametrize(
    "key,value",
    [
        ("TG_API_ID", "not-a-number"),
        ("TG_API_ID", "0"),
        ("TG_CHANNEL_ID", "1.5"),
        ("MAX_ARTIFACT_BYTES", "0"),
        ("MAX_ARTIFACT_BYTES", "-1"),
        ("YTDLP_TIMEOUT_SECONDS", "0"),
        ("YTDLP_TIMEOUT_SECONDS", "inf"),
        ("TG_UPLOAD_TIMEOUT_SECONDS", "-5"),
        ("SHUTDOWN_GRACE_SECONDS", "nonsense"),
        ("ARTIFACT_ROOT", "   "),
    ],
)
def test_unusable_values_are_rejected(
    base_dir: Path, monkeypatch: pytest.MonkeyPatch, key: str, value: str
) -> None:
    monkeypatch.setenv(key, value)
    with pytest.raises(ConfigError, match=key):
        Settings.from_env(base_dir)


def test_fractional_timeouts_are_kept(
    base_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("YTDLP_TIMEOUT_SECONDS", "1.5")
    assert Settings.from_env(base_dir).ytdlp_timeout_seconds == 1.5


@pytest.mark.parametrize("value", ["/", "."])
def test_an_unsafe_artifact_root_is_rejected(
    base_dir: Path, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv("ARTIFACT_ROOT", value)
    with pytest.raises(ConfigError, match="ARTIFACT_ROOT"):
        Settings.from_env(base_dir)


def test_db_path_defaults_relative_to_base_dir(base_dir: Path) -> None:
    settings = Settings.from_env(base_dir)
    assert settings.db_path == base_dir / "yt2tg.sqlite3"


def test_db_path_is_read_from_tg_db_path(
    base_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TG_DB_PATH", "./state/jobs.sqlite3")
    settings = Settings.from_env(base_dir)
    assert settings.db_path == base_dir / "state" / "jobs.sqlite3"


def test_the_session_suffix_is_added_once(
    base_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TG_SESSION_PATH", "./state/bot")
    settings = Settings.from_env(base_dir)
    assert settings.session_path == base_dir / "state" / "bot.session"


def test_the_session_file_is_created_owner_only(base_dir: Path) -> None:
    settings = Settings.from_env(base_dir)
    assert settings.session_path.is_file()
    assert settings.session_path.stat().st_mode & 0o777 == 0o600
    assert settings.session_path.parent.stat().st_mode & 0o777 == 0o700


def test_an_unpreparable_session_path_is_rejected(
    base_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    blocker = base_dir / "blocker"
    blocker.write_text("not a directory")
    monkeypatch.setenv("TG_SESSION_PATH", str(blocker / "bot.session"))
    with pytest.raises(ConfigError, match="TG_SESSION_PATH"):
        Settings.from_env(base_dir)


def test_cookies_are_used_only_when_the_file_exists(
    base_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert Settings.from_env(base_dir).cookies_path is None

    cookies = base_dir / "yt-cookies.txt"
    cookies.write_text("# netscape cookie file")
    assert Settings.from_env(base_dir).cookies_path == cookies

    monkeypatch.setenv("YTDLP_COOKIES_PATH", "  ")
    assert Settings.from_env(base_dir).cookies_path is None


def test_the_cookie_profile_is_looked_up_by_name(
    base_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    looked_up: list[str] = []

    def fake_find(name: str) -> str | None:
        looked_up.append(name)
        return "brave:Profile 3"

    monkeypatch.setattr(config, "find_cookie_profile", fake_find)
    assert Settings.from_env(base_dir).cookies_browser == "brave:Profile 3"
    assert looked_up == ["a2tg"]

    monkeypatch.setenv("YTDLP_COOKIES_PROFILE", "work")
    Settings.from_env(base_dir)
    assert looked_up[-1] == "work"

    monkeypatch.setenv("YTDLP_COOKIES_PROFILE", "  ")
    assert Settings.from_env(base_dir).cookies_browser is None
    assert looked_up[-1] == "work"


def test_secrets_are_kept_out_of_the_repr(base_dir: Path) -> None:
    rendered = repr(Settings.from_env(base_dir))
    assert "hash" not in rendered
    assert "token" not in rendered
