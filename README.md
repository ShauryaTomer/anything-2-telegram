# anything2telegram

Local HTTP server that pushes media into a private Telegram channel.

Give it a YouTube link or a file, it downloads/stages the media locally and uploads it to your channel over MTProto (Telethon), so a *bot* can send files up to ~2 GB instead of the 50 MB Bot HTTP API cap.

You drive it entirely from the FastAPI docs page: start the server, open `/docs`, click through the endpoints.

---

## 1. Requirements

- Python 3.11+
- `ffmpeg` on `PATH` (yt-dlp merges separate video/audio streams)
- `node` on `PATH` (yt-dlp runs YouTube's JS challenge with it)
- A Telegram API app + bot that is an **admin** of the destination channel
- YouTube cookies — see [step 4](#4-youtube-cookies)



## 2. Install

```bash
uv sync
```

## 3. Configure

Create `.env` in the repo root. Real environment variables win over `.env` values.

```dotenv
# required
TG_API_ID=1234567                 # my.telegram.org
TG_API_HASH=abcdef0123456789      # my.telegram.org
TG_BOT_TOKEN=123456:AA...         # @BotFather, bot must be channel admin
TG_CHANNEL_ID=-1001234567890      # @getidsbot

# optional (defaults shown)
TG_TOPIC_ID=                          # forum topic to post into; unset = General
TG_SESSION_PATH=./yt2tg.session
YTDLP_COOKIES_PROFILE=a2tg            # Chrome/Brave profile to read cookies from; see step 4
YTDLP_COOKIES_PATH=./yt-cookies.txt   # fallback when that profile is not found
ARTIFACT_ROOT=/tmp/anything2telegram  # scratch dir for staged/downloaded files
MAX_ARTIFACT_BYTES=2000000000         # hard cap per artifact (~2 GB)
YTDLP_TIMEOUT_SECONDS=3600
TG_UPLOAD_TIMEOUT_SECONDS=3600
SHUTDOWN_GRACE_SECONDS=30
LOG_LEVEL=INFO                        # DEBUG, INFO, WARNING, ERROR, CRITICAL
```

Notes:

- The session file is created with `0600` and its parent directory must not be group/world-writable — the server refuses to start otherwise.
- `ARTIFACT_ROOT` cannot be a filesystem root or the repo root.
- `LOG_LEVEL` applies to this application's loggers only, so `DEBUG` will not also
turn on Telethon's internal chatter. When a job fails, the log line carries the job
id and yt-dlp's own error text; the HTTP response deliberately keeps only a generic
code, since stderr can contain cookie paths and signed URLs.



## 4. YouTube cookies

YouTube now blocks most server-side downloads as bot traffic. Without cookies you will see jobs fail with sign-in / "confirm you're not a bot" errors. Treat cookies as **required in practice**, even though the server starts fine without them.

Cookies are also what unlocks age-restricted, region-locked, private, and members-only videos — a playlist containing them expands normally and each video downloads with your account's access.

Nothing is exported and nothing is stored here: the server hands yt-dlp a browser profile name, and yt-dlp reads that profile's cookie database on every run. Log in once and the browser keeps the session alive from then on.

**macOS only.** On other platforms this falls back to the cookie file in [step 4b](#4b-cookie-file-fallback).

### Set it up once

1. Create a profile named `a2tg` in Chrome or Brave — *profile icon → Add profile* — and call it exactly that.
2. Log in to YouTube in that profile with the account that holds your memberships.
3. Start the server. It logs which profile it found:
  ```
   INFO  Found profile 'a2tg' in brave (directory 'Profile 4')
   INFO  Profile 'a2tg' is signed in to YouTube: yt-dlp will read its cookies as brave:Profile 4
  ```
   Two lines, because finding the profile and finding a login in it are separate things — a profile you created but never logged in to gets a `WARNING` on the second line and still runs, minus members-only.
4. On the first download macOS asks to release the *Brave Safe Storage* (or *Chrome Safe Storage*) key from your keychain. Choose **Always Allow** — yt-dlp needs it to decrypt the cookies, and it will not ask again.

That is all. The browser may stay open while jobs run; yt-dlp copies the cookie database before reading it.

Prefer a different name? `YTDLP_COOKIES_PROFILE=whatever`. Blank turns the lookup off entirely.

### Things that will bite you

- **Sign the membership account in as that profile's *default* account.** yt-dlp uses the profile's first account. If the membership sits on a secondary one, members-only videos fail with a sign-in error and nothing can warn you — this cannot be detected without decrypting the cookies.
- **Don't browse YouTube in the** `a2tg` **profile while jobs run.** YouTube rotates session cookies on use; a download that reads them mid-rotation fails and needs a retry.
- Don't reuse your everyday profile. Google occasionally invalidates a session it sees used from two different clients, which signs you out of your own browser.
- The profile must exist for the user the server runs as, and it is read at **startup** — create it, then start the server.



### Verify

```bash
yt-dlp --cookies-from-browser "brave:Profile 3" --simulate "https://www.youtube.com/watch?v=<members-only-id>"
```

Use the directory name the startup log printed. Then submit one video through `/jobs/youtube` and check the job reaches `completed`.

Symptoms and what they mean:


| Log line                                            | Meaning                                                 |
| --------------------------------------------------- | ------------------------------------------------------- |
| `No Chrome or Brave profile is named 'a2tg'`        | Profile missing, misspelled, or another OS user owns it |
| `Profile 'a2tg' holds no YouTube login cookie`      | Profile found, never logged in — public videos only     |
| `cannot decrypt v10 cookies: no key found` (yt-dlp) | Keychain prompt was denied or never answered            |




### 4b. Cookie file fallback

A browser profile always wins. If none is found, the server falls back to a **Netscape-format** `cookies.txt` — useful on Linux, in Docker, or when you would rather not have a browser involved.

Export it with a browser extension, or from a browser profile:

```bash
yt-dlp --cookies-from-browser "brave:Profile 3" --cookies yt-cookies.txt --skip-download \
  "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
```

Drop it in the repo root as `yt-cookies.txt` (the default path), or point at it:

```dotenv
YTDLP_COOKIES_PATH=/absolute/path/to/yt-cookies.txt
```

Then lock it down — this file is a live login to your Google account:

```bash
chmod 600 yt-cookies.txt
```

Caveats specific to the file:

- Picked up **only if the file exists at startup**. Missing path, no error, no warning — the server just runs without cookies. Restart after replacing it.
- Cookies expire. Expect to re-export every few weeks, sooner if you log out or change your password. Symptom: every YouTube job fails at the `producing` stage while `/health` still reports `ready: true`.
- Never commit it.



## 5. Start the server

```bash
uvicorn anything2telegram.main:app --host 127.0.0.1 --port 8000
```

There is no authentication — bind to `127.0.0.1` only.

Startup connects to Telegram before accepting work. If the config or the Telegram login fails, the process exits with the error.

## 6. Use it from `/docs`

Open [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs). Swagger UI lists every endpoint with a **Try it out** button.

### Check it is alive — `GET /health`

```json
{ "ready": true, "telegram_connected": true }
```

`200` = accepting work. `503` = not ready (still starting, Telegram dropped, or shutting down). Every submit endpoint returns `503` while not ready.

### Send a YouTube video or playlist — `POST /jobs/youtube`

Body:

```json
{ "url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ", "offset": 0 }
```

- `url` — accepted forms:
  - **video**: `watch?v=<id>`, `youtu.be/<id>`, `/shorts/<id>`, `/embed/<id>`, `/v/<id>`
  - **playlist**: `playlist?list=<id>` or `?list=<id>` with no video id
  - A watch URL that also carries `&list=` is treated as a **single video**.
- `offset` — playlists only, skips the first N entries. Ignored for a video URL.

Response `202`:

```json
{ "type": "job", "id": "0d1c…", "status_url": "/jobs/0d1c…" }
```

Playlists return `"type": "batch"` and a `/batches/<id>` status URL.

Bad or non-YouTube URL → `422 unsupported_youtube_url`.

### Send a local file — `POST /jobs/upload`

`multipart/form-data`:


| field     | required | notes                                 |
| --------- | -------- | ------------------------------------- |
| `file`    | yes      | exactly one file                      |
| `caption` | no       | text attached to the Telegram message |


In `/docs` this renders as a file picker plus a caption box.

Response `202` is a job ref, same shape as above.

The upload streams to disk while it is being received, so the request stays open for the whole transfer. Bigger than `MAX_ARTIFACT_BYTES` → `413`; disk full → `507`.

### Poll a job — `GET /jobs/{job_id}`

```json
{
  "id": "0d1c…",
  "batch_id": null,
  "source_kind": "youtube",
  "source": "https://www.youtube.com/watch?v=…",
  "status": "uploading",
  "artifact_id": "9a77…",
  "filename": "video.mp4",
  "size_bytes": 41231234,
  "telegram_message_id": null,
  "error": null,
  "created_at": "2026-07-26T10:00:00Z",
  "updated_at": "2026-07-26T10:01:12Z"
}
```

`status`: `waiting` → `producing` (downloading) → `uploading` → `completed` | `failed`.

On `completed`, `telegram_message_id` is the message in your channel. On `failed`, `error` carries `{code, message}`.

### Poll a playlist — `GET /batches/{batch_id}`

```json
{
  "id": "77ab…",
  "source_url": "https://www.youtube.com/playlist?list=…",
  "status": "processing",
  "total_jobs": 12,
  "waiting": 9, "producing": 1, "uploading": 1, "completed": 1, "failed": 0,
  "skipped_entries": 0,
  "job_ids": ["…"],
  "error": null,
  "created_at": "…", "updated_at": "…"
}
```

`status`: `expanding` (reading the playlist) → `waiting` → `processing` → `completed` | `partially_completed` | `failed`.

`skipped_entries` counts playlist entries that could not be turned into jobs (private, deleted, unavailable). Use `job_ids` to poll individual videos.

## Typical flow

1. `GET /health` → `ready: true`
2. `POST /jobs/youtube` (or `/jobs/upload`) → copy the `id` from the `202`
3. `GET /jobs/{id}` every few seconds until `completed` or `failed`
4. Check your Telegram channel



## Behaviour worth knowing

- **One at a time.** Work runs strictly serially; everything else queues. Submitting a 200-video playlist is fine, it just takes a while.
- **In-memory only.** Job and batch state lives in the process. Restart the server → the queue and all history are gone. Not a durable queue.
- **No retries.** A failed download or upload stays failed; resubmit it yourself.
- **Artifacts are temporary.** Files under `ARTIFACT_ROOT` are deleted after upload, and orphans are swept on startup and shutdown.
- **Graceful shutdown.** `Ctrl+C` stops accepting new work and gives in-flight work `SHUTDOWN_GRACE_SECONDS` to finish before cancelling.
- **Quality cap.** YouTube downloads are capped at 1080p mp4 (h264+m4a where available).



## Error codes


| HTTP | code                                                                                                                 | meaning                                       |
| ---- | -------------------------------------------------------------------------------------------------------------------- | --------------------------------------------- |
| 422  | `unsupported_youtube_url`                                                                                            | URL is not a supported YouTube video/playlist |
| 422  | `invalid_request`                                                                                                    | Malformed body or upload form                 |
| 422  | `invalid_filename`                                                                                                   | Upload filename rejected                      |
| 413  | `staging_oversize`                                                                                                   | File exceeds `MAX_ARTIFACT_BYTES`             |
| 503  | `service_unavailable`                                                                                                | Not ready — check `GET /health`               |
| 507  | `staging_disk_full`                                                                                                  | No space under `ARTIFACT_ROOT`                |
| 500  | `submission_failed`, `upload_staging_failed`, `upload_reservation_failed`, `upload_enqueue_failed`, `internal_error` | Server-side failure, see server logs          |




## Troubleshooting

- **Everything returns 503** — `GET /health`. `telegram_connected: false` means the bot token, api id/hash, or network is the problem; check the server log.
- **Server will not start** — config error text names the offending variable. Session-path errors usually mean the directory is group/world-writable.
- **YouTube job fails immediately** — almost always cookies. Check the startup log for which profile was found, then reproduce with `yt-dlp --cookies-from-browser "<what it printed>" --simulate <url>`. See [step 4](#4-youtube-cookies).
- **YouTube jobs worked yesterday, all fail today** — the profile got signed out of YouTube, or the keychain prompt was denied. Log in again with that browser profile; restart only if you changed the profile name.
- **Upload completes but nothing in the channel** — the bot must be an admin of `TG_CHANNEL_ID`, and the id must be the `-100…` form.



## Development

```bash
pip install -e ".[dev]"
pytest
```

Architecture notes live in [docs/current-architecture.md](docs/current-architecture.md).