# Current Directory Structure

## Repository tree

```text
anything-2-telegram/
├── anything2telegram/                # Installable application package
│   ├── __init__.py
│   ├── main.py                       # FastAPI factory, dependency assembly, lifespan
│   ├── config.py                     # Environment/.env settings and path validation
│   ├── domain.py                     # Shared enums, value objects, snapshots, refs
│   ├── events.py                     # Frozen event payloads and topic constants
│   ├── api/
│   │   ├── __init__.py
│   │   └── jobs.py                   # HTTP endpoints, schemas, multipart handling
│   ├── artifacts/
│   │   ├── __init__.py
│   │   ├── storage.py                # Safe reserve/stage/validate/measure/delete API
│   │   └── cleanup.py                # Terminal-event cleanup listener
│   ├── downloaders/
│   │   ├── __init__.py
│   │   ├── process.py                # Cancellable process-group runner
│   │   └── youtube.py                # URL classification, playlist/download producer
│   ├── jobs/
│   │   ├── __init__.py
│   │   ├── scheduler.py              # Single-active FIFO scheduler
│   │   └── tracker.py                # Job/batch lifecycle projections
│   └── telegram/
│       ├── __init__.py
│       ├── client.py                 # Telethon adapter and typed failures
│       └── uploader.py               # Generic artifact.ready consumer
├── api/                              # Backward-compatible import shim; no current importers
│   ├── __init__.py
│   └── jobs.py
├── tests/
│   ├── conftest.py                    # Shared fakes: FakeYouTubeRunner, FakeTelegram, build_app
│   ├── api/test_jobs.py
│   ├── artifacts/
│   │   ├── test_cleanup.py
│   │   └── test_storage.py
│   ├── downloaders/
│   │   ├── test_process.py
│   │   └── test_youtube.py
│   ├── integration/
│   │   ├── test_choreography.py
│   │   └── test_unavailable.py
│   ├── jobs/
│   │   ├── test_scheduler.py
│   │   └── test_tracker.py
│   ├── telegram/
│   │   ├── test_client.py
│   │   └── test_uploader.py
│   ├── test_config.py
│   └── test_lifespan.py
├── docs/
│   ├── _work/
│   │   └── event-driven-server.md    # Original design decisions and implementation plan
│   ├── ARCHITECTURE.md               # Module-by-module explanation, bus mechanics, principles
│   ├── HAPPY-PATHS.md                # The six end-to-end flows and what each one guarantees
│   ├── current-architecture.md       # C4-style system/container/component views
│   └── current-directory-structure.md # This document
├── lessons/                          # Local teaching material; Git-ignored
├── reference/                        # Local event/pyee reference material; Git-ignored
├── pyproject.toml                    # Package metadata and dependency bounds
├── requirements.lock                 # Resolved dependency versions
└── yt_2_tg.py                        # Original pre-server script retained for reference
```

Generated/local directories such as `.venv/`, `build/`, `dist/`, caches, artifact storage, Telegram sessions, cookies, and secrets are intentionally omitted.

## Package responsibilities

| Path | Responsibility | May depend on |
|---|---|---|
| `anything2telegram/domain.py` | Shared domain vocabulary and immutable return models | Standard library |
| `anything2telegram/events.py` | Event names and frozen payload contracts | Domain types |
| `anything2telegram/config.py` | Runtime configuration and safe path resolution | Environment and filesystem |
| `anything2telegram/jobs/scheduler.py` | Admission, FIFO order, one-active invariant, command emission | Events, storage, event bus |
| `anything2telegram/jobs/tracker.py` | Read-only job/batch projections from facts | Events and domain snapshots |
| `anything2telegram/artifacts/storage.py` | Temporary-file ownership and filesystem safety | Local filesystem |
| `anything2telegram/artifacts/cleanup.py` | Terminal artifact deletion | Storage and upload facts |
| `anything2telegram/downloaders/youtube.py` | YouTube-specific production logic | Generic events, storage, process adapter |
| `anything2telegram/downloaders/process.py` | Process timeout/cancellation/group cleanup | asyncio and OS process APIs |
| `anything2telegram/telegram/client.py` | Telethon isolation | Telethon |
| `anything2telegram/telegram/uploader.py` | Generic artifact upload consumer | Generic events, storage, Telegram adapter |
| `anything2telegram/api/jobs.py` | HTTP validation/serialization and multipart staging | Scheduler, tracker, storage — read from `app.state` |
| `anything2telegram/main.py` | Concrete dependency wiring and lifecycle ownership | All application components |

Components are passed to each other as constructor arguments and published on `app.state`. There are
no `Protocol` types, factories, or injection containers: the two seams that tests need
(`YouTubeProcessRunner`, `TelegramClientAdapter`) are patched as module attributes of `main`.

## Dependency direction

```text
main/lifecycle
  ├── API ───────────────> scheduler, tracker, storage (via app.state)
  ├── scheduler ─────────> shared events + storage
  ├── YouTube producer ──> shared events + storage + process runner
  ├── Telegram uploader ─> shared events + storage + client adapter
  ├── tracker ───────────> shared events + domain snapshots
  └── cleanup ───────────> terminal events + storage

YouTube producer -X-> Telegram uploader
Telegram uploader -X-> YouTube producer
```

`-X->` means the dependency is intentionally forbidden. Communication crosses the shared event contracts instead.

## Test organization

- Module tests mirror production package boundaries.
- Integration choreography uses real application components and fake external adapters.
- Shared fakes live in `tests/conftest.py`; no production code exists to serve a test.
- External Telegram and YouTube smoke testing remains manual because it creates network side effects.

## Git behavior

- `docs/_work/`, `lessons/`, and `reference/` are Git-ignored.
- `docs/*.md` is tracked; the rest of `docs/_work/` stays local.
