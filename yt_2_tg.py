#!/home/glitch/.venvs/yt2tg/bin/python3
"""Download YouTube video(s) and upload them to a private Telegram channel.

Usage:
    ./yt_2_tg.py <youtube-url> [<youtube-url> ...]

A URL with a video id (watch?v=..., youtu.be/..., /shorts/...) downloads that
one video, even if it also carries a &list=. A bare playlist URL
(playlist?list=... or list=... with no video id) downloads every video in it.

Required environment variables:
    TG_API_ID       from https://my.telegram.org
    TG_API_HASH     from https://my.telegram.org
    TG_BOT_TOKEN    from @BotFather (bot must be admin of the channel)
    TG_CHANNEL_ID   e.g. -1001234567890  (from @getidsbot)

Notes:
    * Uses MTProto (Telethon) so a *bot* can upload up to ~2 GB,
      bypassing the 50 MB Bot HTTP API limit.
    * Requires: yt-dlp, ffmpeg, and `pip install telethon`.
"""

import asyncio
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from dotenv import load_dotenv
from telethon import TelegramClient

# Load secrets from a .env file sitting next to this script, regardless of cwd.
load_dotenv(Path(__file__).with_name(".env"))

API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
BOT_TOKEN = os.environ["TG_BOT_TOKEN"]
# Must be an int: passing the channel ID as a string makes Telethon try to
# resolve it as a username/phone, which bots aren't allowed to do.
CHANNEL = int(os.environ["TG_CHANNEL_ID"])

# Prefer an H.264/AAC mp4 so Telegram plays it inline without transcoding.
YTDLP_FORMAT = "bv*[ext=mp4][vcodec^=avc1][height<=1080]+ba[ext=m4a]/b[ext=mp4][height<=1080]/b[height<=1080]"

# Optional Netscape-format cookies file (from a logged-in YouTube session).
# If cookies.txt sits next to this script, yt-dlp uses it automatically so
# age-restricted / private / "confirm you're not a bot" videos work.
COOKIES = Path(__file__).with_name("yt-cookies.txt")


def cookie_args() -> list[str]:
    """--cookies flag if a cookie file sits next to this script, else nothing."""
    return ["--cookies", str(COOKIES)] if COOKIES.exists() else []


def expand_targets(url: str) -> list[str]:
    """Turn one input URL into the list of single-video URLs to download.

    A URL carrying a video id is a single video (even with a &list=); a bare
    playlist URL is expanded into one watch URL per entry.
    """
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    host = parsed.netloc.lower()

    has_video_id = (
        "v" in query
        or host.endswith("youtu.be")
        or parsed.path.startswith(("/shorts/", "/embed/", "/v/"))
    )
    if has_video_id:
        return [url]

    if "list" in query or parsed.path.startswith("/playlist"):
        return playlist_entries(url)

    # Channel pages, etc. — hand back as-is and let the download step handle it.
    return [url]


def playlist_entries(url: str) -> list[str]:
    """Flat-list a playlist into individual watch URLs (no format extraction)."""
    cmd = ["yt-dlp", "--flat-playlist", "--print", "%(id)s"] + cookie_args() + [url]
    out = subprocess.run(cmd, check=True, capture_output=True, text=True)
    ids = [line.strip() for line in out.stdout.splitlines() if line.strip()]
    return [f"https://www.youtube.com/watch?v={vid}" for vid in ids]


def download(url: str, outdir: Path) -> Path:
    before = set(outdir.glob("*"))
    cmd = [
        "yt-dlp",
        "-f", YTDLP_FORMAT,
        "--merge-output-format", "mp4",
        "--restrict-filenames",
        # We expand playlists ourselves, so each download is a single video.
        "--no-playlist",
        # YouTube's "n" signature challenge needs a JS runtime + solver scripts.
        # Use the installed Node (>=22) and fetch the EJS solver from GitHub
        # (cached after first run). Without this, only storyboard images resolve.
        "--js-runtimes", "node",
        "--remote-components", "ejs:github",
        "-o", str(outdir / "%(title).80s.%(ext)s"),
    ] + cookie_args() + [url]
    subprocess.run(cmd, check=True)
    new_files = [f for f in outdir.glob("*") if f not in before and f.is_file()]
    videos = [f for f in new_files if f.suffix.lower() in (".mp4", ".mkv", ".webm")]
    if not videos:
        raise RuntimeError(f"yt-dlp produced no video file for {url}")
    return max(videos, key=lambda f: f.stat().st_size)


def _progress(name: str):
    def cb(sent: int, total: int):
        pct = sent / total * 100 if total else 0
        print(f"\r  uploading {name}: {pct:5.1f}%", end="", flush=True)
    return cb


async def main(urls: list[str]):
    client = TelegramClient("yt2tg", API_ID, API_HASH)
    await client.start(bot_token=BOT_TOKEN)
    try:
        for url in urls:
            targets = expand_targets(url)
            if len(targets) != 1:
                print(f"playlist detected: {len(targets)} video(s)")

            total = len(targets)
            for i, target in enumerate(targets, 1):
                prefix = f"[{i}/{total}] " if total > 1 else ""
                try:
                    with tempfile.TemporaryDirectory() as tmp:
                        tmp_path = Path(tmp)
                        print(f"{prefix}downloading: {target}")
                        video = download(target, tmp_path)
                        size_mb = video.stat().st_size / 1e6
                        print(f"  got {video.name} ({size_mb:.1f} MB)")

                        if size_mb > 2000:
                            print("  ! larger than 2 GB — a bot can't upload this. "
                                  "Use a user session instead. Skipping.")
                            continue

                        await client.send_file(
                            CHANNEL,
                            str(video),
                            caption=video.stem.replace("_", " "),
                            supports_streaming=True,
                            progress_callback=_progress(video.name),
                        )
                        print("  done.")
                except subprocess.CalledProcessError as exc:
                    print(f"  ! yt-dlp failed for {target} (exit {exc.returncode}). "
                          "Skipping.")
                except Exception as exc:  # keep going through the rest of a playlist
                    print(f"  ! error on {target}: {exc}. Skipping.")
    finally:
        await client.disconnect()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("usage: yt2tg.py <youtube-url> [<youtube-url> ...]")
    asyncio.run(main(sys.argv[1:]))
