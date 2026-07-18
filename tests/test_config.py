import stat
from dataclasses import replace
from pathlib import Path

import pytest

from anything2telegram.config import ConfigError, Settings


def required_env(**overrides: str) -> dict[str, str]:
    values = {
        "TG_API_ID": "12345",
        "TG_API_HASH": "api-hash",
        "TG_BOT_TOKEN": "bot-token",
        "TG_CHANNEL_ID": "-100123456789",
    }
    values.update(overrides)
    return values


def test_defaults_are_typed_and_paths_resolve_from_base_dir(tmp_path: Path) -> None:
    settings = Settings.from_env(tmp_path, required_env())

    assert settings.api_id == 12345
    assert settings.api_hash == "api-hash"
    assert settings.bot_token == "bot-token"
    assert settings.channel_id == -100123456789
    assert settings.session_path == tmp_path / "yt2tg.session"
    assert settings.cookies_path is None
    assert settings.artifact_root == Path("/tmp/anything2telegram")
    assert settings.max_artifact_bytes == 2_000_000_000
    assert settings.ytdlp_timeout_seconds == 3600
    assert settings.tg_upload_timeout_seconds == 3600
    assert settings.shutdown_grace_seconds == 30
    assert isinstance(settings.api_id, int)
    assert isinstance(settings.channel_id, int)
    assert isinstance(settings.max_artifact_bytes, int)
    assert isinstance(settings.ytdlp_timeout_seconds, (int, float))


def test_dotenv_is_read_but_supplied_environment_wins(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text(
        "TG_API_ID=1\n"
        "TG_API_HASH=from-file\n"
        "TG_BOT_TOKEN=file-token\n"
        "TG_CHANNEL_ID=-1001\n"
        "MAX_ARTIFACT_BYTES=15\n"
    )

    settings = Settings.from_env(
        tmp_path,
        {
            "TG_API_ID": "2",
            "TG_API_HASH": "from-environ",
            "TG_BOT_TOKEN": "env-token",
            "TG_CHANNEL_ID": "-1002",
        },
    )

    assert settings.api_id == 2
    assert settings.api_hash == "from-environ"
    assert settings.bot_token == "env-token"
    assert settings.channel_id == -1002
    assert settings.max_artifact_bytes == 15


def test_os_environment_wins_when_no_mapping_is_supplied(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / ".env").write_text(
        "TG_API_ID=1\nTG_API_HASH=file\nTG_BOT_TOKEN=file\nTG_CHANNEL_ID=-1\n"
    )
    for key, value in required_env(TG_API_ID="99").items():
        monkeypatch.setenv(key, value)

    settings = Settings.from_env(tmp_path)

    assert settings.api_id == 99


def test_relative_session_and_cookie_paths_use_base_dir(tmp_path: Path) -> None:
    cookie = tmp_path / "private" / "cookies.txt"
    cookie.parent.mkdir()
    cookie.touch()

    settings = Settings.from_env(
        tmp_path,
        required_env(
            TG_SESSION_PATH="sessions/telegram.session",
            YTDLP_COOKIES_PATH="private/cookies.txt",
        ),
    )

    assert settings.session_path == tmp_path / "sessions" / "telegram.session"
    assert settings.cookies_path == cookie
    assert settings.session_path.parent.is_dir()


def test_absolute_paths_remain_absolute(tmp_path: Path) -> None:
    cookie = tmp_path / "absolute-cookies.txt"
    cookie.touch()
    session = tmp_path / "absolute" / "session"
    artifact_root = tmp_path.parent / f"{tmp_path.name}-artifacts"

    settings = Settings.from_env(
        tmp_path,
        required_env(
            TG_SESSION_PATH=str(session),
            YTDLP_COOKIES_PATH=str(cookie),
            ARTIFACT_ROOT=str(artifact_root),
        ),
    )

    assert settings.session_path == Path(f"{session}.session")
    assert settings.cookies_path == cookie
    assert settings.artifact_root == artifact_root


def test_missing_optional_cookie_file_becomes_none(tmp_path: Path) -> None:
    settings = Settings.from_env(
        tmp_path,
        required_env(YTDLP_COOKIES_PATH="missing/cookies.txt"),
    )
    assert settings.cookies_path is None


@pytest.mark.parametrize("key", ["TG_API_ID", "TG_API_HASH", "TG_BOT_TOKEN", "TG_CHANNEL_ID"])
def test_required_values_must_be_present_and_error_names_only_key(
    tmp_path: Path, key: str
) -> None:
    environ = required_env()
    del environ[key]

    with pytest.raises(ConfigError) as raised:
        Settings.from_env(tmp_path, environ)

    assert key in str(raised.value)


@pytest.mark.parametrize("key", ["TG_API_ID", "TG_API_HASH", "TG_BOT_TOKEN", "TG_CHANNEL_ID"])
def test_required_values_must_not_be_blank(tmp_path: Path, key: str) -> None:
    secret = "  "
    with pytest.raises(ConfigError) as raised:
        Settings.from_env(tmp_path, required_env(**{key: secret}))

    assert key in str(raised.value)
    assert repr(secret) not in str(raised.value)


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("TG_API_ID", "not-an-int"),
        ("TG_CHANNEL_ID", "1.5"),
        ("MAX_ARTIFACT_BYTES", "huge"),
        ("YTDLP_TIMEOUT_SECONDS", "slow"),
        ("TG_UPLOAD_TIMEOUT_SECONDS", "slow"),
        ("SHUTDOWN_GRACE_SECONDS", "slow"),
    ],
)
def test_invalid_numbers_raise_safe_errors(tmp_path: Path, key: str, value: str) -> None:
    with pytest.raises(ConfigError) as raised:
        Settings.from_env(tmp_path, required_env(**{key: value}))

    assert key in str(raised.value)
    assert value not in str(raised.value)
    assert raised.value.__cause__ is None


@pytest.mark.parametrize("value", ["0", "-1"])
def test_api_id_must_be_positive_and_error_is_safe(
    tmp_path: Path, value: str
) -> None:
    with pytest.raises(ConfigError) as raised:
        Settings.from_env(tmp_path, required_env(TG_API_ID=value))

    assert "TG_API_ID" in str(raised.value)
    assert value not in str(raised.value)


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("MAX_ARTIFACT_BYTES", "0"),
        ("MAX_ARTIFACT_BYTES", "-1"),
        ("YTDLP_TIMEOUT_SECONDS", "0"),
        ("TG_UPLOAD_TIMEOUT_SECONDS", "-1"),
        ("SHUTDOWN_GRACE_SECONDS", "0"),
    ],
)
def test_sizes_and_timeouts_must_be_positive(
    tmp_path: Path, key: str, value: str
) -> None:
    with pytest.raises(ConfigError, match=key):
        Settings.from_env(tmp_path, required_env(**{key: value}))


@pytest.mark.parametrize("unsafe_root", [Path("/"), Path(".")])
def test_artifact_root_rejects_filesystem_and_repository_roots(
    tmp_path: Path, unsafe_root: Path
) -> None:
    value = str(tmp_path if unsafe_root == Path(".") else unsafe_root)
    with pytest.raises(ConfigError, match="ARTIFACT_ROOT"):
        Settings.from_env(tmp_path, required_env(ARTIFACT_ROOT=value))


def test_artifact_root_rejects_symlink_to_filesystem_root(tmp_path: Path) -> None:
    root_link = tmp_path / "root-link"
    root_link.symlink_to(Path("/"), target_is_directory=True)

    with pytest.raises(ConfigError, match="ARTIFACT_ROOT"):
        Settings.from_env(
            tmp_path,
            required_env(ARTIFACT_ROOT=str(root_link)),
        )


def test_settings_are_frozen(tmp_path: Path) -> None:
    settings = Settings.from_env(tmp_path, required_env())
    with pytest.raises(AttributeError):
        settings.api_id = 999  # type: ignore[misc]


def test_settings_repr_redacts_telegram_secrets(tmp_path: Path) -> None:
    api_hash = "highly-sensitive-api-hash"
    bot_token = "highly-sensitive-bot-token"

    settings = Settings.from_env(
        tmp_path,
        required_env(TG_API_HASH=api_hash, TG_BOT_TOKEN=bot_token),
    )

    assert api_hash not in repr(settings)
    assert bot_token not in repr(settings)


def test_invalid_later_config_does_not_mutate_session_filesystem(
    tmp_path: Path,
) -> None:
    session_path = tmp_path / "new-private-parent" / "telegram.session"

    with pytest.raises(ConfigError, match="MAX_ARTIFACT_BYTES"):
        Settings.from_env(
            tmp_path,
            required_env(
                TG_SESSION_PATH=str(session_path),
                MAX_ARTIFACT_BYTES="invalid",
            ),
        )

    assert not session_path.parent.exists()
    assert not session_path.exists()


def test_new_session_parent_and_file_get_private_modes(tmp_path: Path) -> None:
    session_path = tmp_path / "new-private-parent" / "telegram.session"

    Settings.from_env(
        tmp_path,
        required_env(TG_SESSION_PATH=str(session_path)),
    )

    assert stat.S_IMODE(session_path.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(session_path.stat().st_mode) == 0o600


def test_existing_session_file_is_restricted_without_chmodding_base_dir(
    tmp_path: Path,
) -> None:
    session_path = tmp_path / "telegram.session"
    session_path.touch(mode=0o644)
    session_path.chmod(0o644)
    tmp_path.chmod(0o755)

    Settings.from_env(
        tmp_path,
        required_env(TG_SESSION_PATH=str(session_path)),
    )

    assert stat.S_IMODE(session_path.stat().st_mode) == 0o600
    assert stat.S_IMODE(tmp_path.stat().st_mode) == 0o755


def test_session_filesystem_error_is_safe(tmp_path: Path) -> None:
    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory")
    session_path = blocker / "secret-session-name"

    with pytest.raises(ConfigError) as raised:
        Settings.from_env(
            tmp_path,
            required_env(TG_SESSION_PATH=str(session_path)),
        )

    assert "TG_SESSION_PATH" in str(raised.value)
    assert str(session_path) not in str(raised.value)
    assert "secret-session-name" not in str(raised.value)


@pytest.mark.parametrize(
    ("configured_name", "effective_name"),
    [
        ("telegram", "telegram.session"),
        ("telegram.sqlite", "telegram.sqlite.session"),
        ("telegram.SESSION", "telegram.SESSION.session"),
        ("telegram.session", "telegram.session"),
    ],
)
def test_session_path_matches_telethon_suffix_rules(
    tmp_path: Path, configured_name: str, effective_name: str
) -> None:
    settings = Settings.from_env(
        tmp_path,
        required_env(TG_SESSION_PATH=configured_name),
    )

    assert settings.session_path == tmp_path / effective_name
    assert settings.session_path.is_file()


def test_settings_session_path_is_the_sqlite_file_telethon_uses(
    tmp_path: Path,
) -> None:
    from telethon.sessions import SQLiteSession

    configured_base = tmp_path / "sessions" / "telegram"
    settings = Settings.from_env(
        tmp_path,
        required_env(TG_SESSION_PATH=str(configured_base)),
    )

    session = SQLiteSession(str(configured_base))
    session.close()

    assert settings.session_path == Path(session.filename)
    assert settings.session_path.is_file()
    assert stat.S_IMODE(settings.session_path.stat().st_mode) == 0o600


def test_session_path_symlink_is_rejected_without_chmodding_target(
    tmp_path: Path,
) -> None:
    target = tmp_path / "real.session"
    target.touch(mode=0o644)
    target.chmod(0o644)
    link = tmp_path / "linked.session"
    link.symlink_to(target)

    with pytest.raises(ConfigError) as raised:
        Settings.from_env(
            tmp_path,
            required_env(TG_SESSION_PATH=str(link)),
        )

    assert "TG_SESSION_PATH" in str(raised.value)
    assert str(link) not in str(raised.value)
    assert stat.S_IMODE(target.stat().st_mode) == 0o644


@pytest.mark.parametrize("mode", [0o770, 0o707])
def test_existing_writable_session_parent_is_rejected(
    tmp_path: Path, mode: int
) -> None:
    parent = tmp_path / "unsafe-parent"
    parent.mkdir()
    parent.chmod(mode)
    session_path = parent / "telegram.session"

    with pytest.raises(ConfigError, match="TG_SESSION_PATH"):
        Settings.from_env(
            tmp_path,
            required_env(TG_SESSION_PATH=str(session_path)),
        )

    assert not session_path.exists()
    assert stat.S_IMODE(parent.stat().st_mode) == mode


def valid_settings(tmp_path: Path) -> Settings:
    return Settings(
        api_id=12345,
        api_hash="api-hash",
        bot_token="bot-token",
        channel_id=-100123,
        session_path=tmp_path / "telegram.session",
        cookies_path=None,
        artifact_root=tmp_path / "artifacts",
        max_artifact_bytes=100,
        ytdlp_timeout_seconds=1,
        tg_upload_timeout_seconds=1.5,
        shutdown_grace_seconds=1,
    )


@pytest.mark.parametrize(
    ("field_name", "invalid_value", "config_key"),
    [
        ("api_id", True, "TG_API_ID"),
        ("api_id", 1.5, "TG_API_ID"),
        ("api_hash", 7, "TG_API_HASH"),
        ("api_hash", " ", "TG_API_HASH"),
        ("bot_token", 7, "TG_BOT_TOKEN"),
        ("bot_token", "", "TG_BOT_TOKEN"),
        ("channel_id", True, "TG_CHANNEL_ID"),
        ("channel_id", 1.5, "TG_CHANNEL_ID"),
        ("session_path", "/tmp/session", "TG_SESSION_PATH"),
        ("cookies_path", "/tmp/cookies", "YTDLP_COOKIES_PATH"),
        ("artifact_root", "/tmp/artifacts", "ARTIFACT_ROOT"),
        ("max_artifact_bytes", True, "MAX_ARTIFACT_BYTES"),
        ("max_artifact_bytes", 1.5, "MAX_ARTIFACT_BYTES"),
        ("ytdlp_timeout_seconds", True, "YTDLP_TIMEOUT_SECONDS"),
        ("ytdlp_timeout_seconds", "1", "YTDLP_TIMEOUT_SECONDS"),
        ("ytdlp_timeout_seconds", float("nan"), "YTDLP_TIMEOUT_SECONDS"),
        ("tg_upload_timeout_seconds", True, "TG_UPLOAD_TIMEOUT_SECONDS"),
        ("tg_upload_timeout_seconds", float("inf"), "TG_UPLOAD_TIMEOUT_SECONDS"),
        ("shutdown_grace_seconds", "1", "SHUTDOWN_GRACE_SECONDS"),
        ("shutdown_grace_seconds", 0, "SHUTDOWN_GRACE_SECONDS"),
    ],
)
def test_direct_settings_rejects_invalid_runtime_field_values(
    tmp_path: Path,
    field_name: str,
    invalid_value: object,
    config_key: str,
) -> None:
    with pytest.raises(ConfigError, match=config_key):
        replace(valid_settings(tmp_path), **{field_name: invalid_value})
