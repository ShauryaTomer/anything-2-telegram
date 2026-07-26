"""Locate the browser profile yt-dlp should read YouTube cookies from.

yt-dlp does every part of the cookie work itself — finding the database,
copying it out from under the running browser, reading the macOS keychain,
decrypting each value. All it needs is `browser:profile`, and the profile part
is the on-disk directory name (`Profile 3`), not the name the user typed when
they created it. Chromium keeps that display name in `Local State`, so
translating one into the other is the only job here.
"""

import contextlib
import json
import logging
import shutil
import sqlite3
import sys
import tempfile
from pathlib import Path


_LOGGER = logging.getLogger(__name__)

# yt-dlp's own table of browser directories, narrowed to the two we support.
_BROWSER_DIRS = {
    "chrome": "Google/Chrome",
    "brave": "BraveSoftware/Brave-Browser",
}
# YouTube sets this one only for a signed-in session.
_LOGIN_COOKIE = "LOGIN_INFO"


def find_cookie_profile(name: str, *, home: Path | None = None) -> str | None:
    """Return `browser:directory` for the profile displayed as `name`.

    Returns None when no such profile exists, which leaves the caller on its
    cookie-file fallback. A profile that exists but is signed out is still
    returned — its cookies get age-restricted videos through even though
    members-only will fail — and warned about instead.
    """
    if sys.platform != "darwin":
        _LOGGER.debug("Browser cookie profiles are only located on macOS")
        return None
    support = (home or Path.home()) / "Library/Application Support"
    matches = [
        (browser, directory)
        for browser, relative in _BROWSER_DIRS.items()
        for directory in _profiles_named(support / relative, name)
    ]
    if not matches:
        _LOGGER.warning(
            "No Chrome or Brave profile is named %r, so yt-dlp gets no browser "
            "cookies. Create the profile or set YTDLP_COOKIES_PROFILE.",
            name,
        )
        return None

    signed_in = [match for match in matches if _is_signed_into_youtube(match[1])]
    candidates = signed_in or matches
    browser, directory = candidates[0]
    # Finding the profile is only half of it — an empty one looks identical from
    # here — so say which step succeeded and which did not.
    _LOGGER.info(
        "Found profile %r in %s (directory %r)%s",
        name,
        browser,
        directory.name,
        f", chosen out of {len(candidates)} of that name" if len(candidates) > 1 else "",
    )
    if signed_in:
        _LOGGER.info(
            "Profile %r is signed in to YouTube: yt-dlp will read its cookies "
            "as %s:%s",
            name,
            browser,
            directory.name,
        )
    else:
        _LOGGER.warning(
            "Profile %r holds no YouTube login cookie. Using %s:%s anyway, but "
            "members-only and age-restricted videos will fail until you open "
            "that profile and log in to YouTube.",
            name,
            browser,
            directory.name,
        )
    return f"{browser}:{directory.name}"


def _profiles_named(root: Path, name: str) -> list[Path]:
    try:
        state = json.loads((root / "Local State").read_text(encoding="utf8"))
        return [
            root / directory
            for directory, profile in state["profile"]["info_cache"].items()
            if profile["name"] == name and (root / directory / "Cookies").is_file()
        ]
    except (OSError, ValueError, LookupError, AttributeError, TypeError):
        # The browser is not installed, or its Local State is not what we expect.
        return []


def _is_signed_into_youtube(directory: Path) -> bool:
    """Whether the profile holds a YouTube login cookie.

    Cookie names and hosts are stored unencrypted, so this needs no keychain
    access and cannot trigger the macOS permission dialog — only the values
    yt-dlp later decrypts are protected.
    """
    with tempfile.TemporaryDirectory(prefix="a2tg") as tmpdir:
        copy = Path(tmpdir) / "Cookies"
        try:
            # The live database is locked by the running browser.
            shutil.copy(directory / "Cookies", copy)
            with contextlib.closing(sqlite3.connect(copy)) as connection:
                return (
                    connection.execute(
                        "SELECT 1 FROM cookies"
                        " WHERE name = ? AND host_key LIKE '%youtube.com' LIMIT 1",
                        (_LOGIN_COOKIE,),
                    ).fetchone()
                    is not None
                )
        except (OSError, sqlite3.Error):
            return False
