import json
import sqlite3
import sys
from pathlib import Path

import pytest

from anything2telegram.browsers import find_cookie_profile


BRAVE = "Library/Application Support/BraveSoftware/Brave-Browser"
CHROME = "Library/Application Support/Google/Chrome"


@pytest.fixture(autouse=True)
def macos(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "platform", "darwin")


def add_profile(
    home: Path,
    relative: str,
    directory: str,
    name: str,
    *,
    cookies: str | None = "signed-in",
) -> None:
    """Create the Local State entry and cookie database a browser would write."""
    root = home / relative
    root.mkdir(parents=True, exist_ok=True)
    state_path = root / "Local State"
    state = (
        json.loads(state_path.read_text())
        if state_path.is_file()
        else {"profile": {"info_cache": {}}}
    )
    state["profile"]["info_cache"][directory] = {"name": name}
    state_path.write_text(json.dumps(state))
    if cookies is None:
        return
    (root / directory).mkdir(exist_ok=True)
    with sqlite3.connect(root / directory / "Cookies") as connection:
        connection.execute("CREATE TABLE cookies (name TEXT, host_key TEXT)")
        insert = "INSERT INTO cookies VALUES (?, '.youtube.com')"
        connection.execute(insert, ("VISITOR_INFO1_LIVE",))
        if cookies == "signed-in":
            connection.execute(insert, ("LOGIN_INFO",))


def test_the_named_profile_resolves_to_its_directory(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    add_profile(tmp_path, BRAVE, "Default", "Personal")
    add_profile(tmp_path, BRAVE, "Profile 3", "a2tg")

    with caplog.at_level("INFO"):
        assert find_cookie_profile("a2tg", home=tmp_path) == "brave:Profile 3"
    assert "Found profile 'a2tg' in brave (directory 'Profile 3')" in caplog.text
    assert "is signed in to YouTube" in caplog.text


def test_chrome_is_searched_too(tmp_path: Path) -> None:
    add_profile(tmp_path, CHROME, "Profile 1", "a2tg")

    assert find_cookie_profile("a2tg", home=tmp_path) == "chrome:Profile 1"


def test_no_matching_profile_leaves_the_caller_without_a_browser(
    tmp_path: Path,
) -> None:
    add_profile(tmp_path, BRAVE, "Default", "Personal")

    assert find_cookie_profile("a2tg", home=tmp_path) is None
    assert find_cookie_profile("a2tg", home=tmp_path / "empty") is None


def test_a_profile_without_a_cookie_database_is_not_offered(tmp_path: Path) -> None:
    add_profile(tmp_path, BRAVE, "Profile 3", "a2tg", cookies=None)

    assert find_cookie_profile("a2tg", home=tmp_path) is None


def test_a_signed_out_profile_is_used_but_warned_about(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    add_profile(tmp_path, BRAVE, "Profile 3", "a2tg", cookies="signed-out")

    with caplog.at_level("INFO"):
        assert find_cookie_profile("a2tg", home=tmp_path) == "brave:Profile 3"
    assert "Found profile 'a2tg' in brave" in caplog.text
    assert "holds no YouTube login cookie" in caplog.text


def test_a_duplicated_name_prefers_the_signed_in_profile(tmp_path: Path) -> None:
    add_profile(tmp_path, CHROME, "Profile 1", "a2tg", cookies="signed-out")
    add_profile(tmp_path, BRAVE, "Profile 3", "a2tg")

    assert find_cookie_profile("a2tg", home=tmp_path) == "brave:Profile 3"


def test_unreadable_local_state_is_ignored(tmp_path: Path) -> None:
    (tmp_path / BRAVE).mkdir(parents=True)
    (tmp_path / BRAVE / "Local State").write_text("{not json")

    assert find_cookie_profile("a2tg", home=tmp_path) is None


def test_other_platforms_have_no_browser_profiles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    add_profile(tmp_path, BRAVE, "Profile 3", "a2tg")
    monkeypatch.setattr(sys, "platform", "linux")

    assert find_cookie_profile("a2tg", home=tmp_path) is None
