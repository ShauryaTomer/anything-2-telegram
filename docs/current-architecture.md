# Current Architecture

## Scope

- Local Python server accepting YouTube URLs and multipart file uploads.
- Files become local artifacts before Telegram upload.
- Modules communicate through typed `pyee` events.
- One active work item at a time; remaining work waits in memory.
- No authentication, durable queue, retry, crash recovery, or multi-process coordination.

## System context

```mermaid
flowchart TB
  user["👤 Local client<br/><small>Submits URLs/files and reads job status</small>"]
  server["Anything to Telegram<br/><small>Local event-driven upload server</small>"]
  youtube["YouTube<br/><small>Video and playlist media source</small>"]
  telegram["Telegram<br/><small>Destination for uploaded artifacts</small>"]

  user -->|"Submits and monitors jobs<br/>HTTP/JSON + multipart"| server
  server -->|"Downloads media using yt-dlp<br/>HTTPS"| youtube
  server -->|"Uploads artifacts using Telethon<br/>MTProto"| telegram

  classDef ext fill:#8a8a8a,stroke:#666,color:#fff
  classDef sys fill:#2a6fdb,stroke:#1a4fa0,color:#fff
  class server sys
  class youtube,telegram ext
```

## Container view

```mermaid
flowchart TB
  user["👤 Local client<br/><small>CLI, browser, or another local program</small>"]
  youtube["YouTube<br/><small>Media source</small>"]
  telegram["Telegram<br/><small>Upload destination</small>"]

  subgraph system["Anything to Telegram"]
    server["Python server<br/><small>FastAPI, asyncio, pyee</small><br/><small>HTTP API, scheduling, events, tracking, lifecycle</small>"]
    process["yt-dlp process<br/><small>Python module subprocess</small><br/><small>Playlist expansion and media download</small>"]
    artifacts[("Artifact directory<br/><small>Local filesystem</small><br/><small>Temporary staged and downloaded files</small>")]
  end

  user -->|"Creates and queries jobs<br/>HTTP on 127.0.0.1"| server
  server -->|"Starts, times out, and cancels<br/>Subprocess argv"| process
  process -->|"Fetches metadata/media<br/>HTTPS"| youtube
  process -->|"Writes bounded temporary media"| artifacts
  server -->|"Stages, validates, and deletes<br/>FD-relative filesystem ops"| artifacts
  server -->|"Uploads validated artifacts<br/>Telethon/MTProto"| telegram

  classDef ext fill:#8a8a8a,stroke:#666,color:#fff
  classDef cont fill:#4a86e8,stroke:#2a66c8,color:#fff
  class server,process,artifacts cont
  class youtube,telegram ext
```

## Component view

```mermaid
flowchart TB
  subgraph server["FastAPI process"]
    api["HTTP API<br/><small>FastAPI</small><br/><small>Validates requests — job, batch, upload, health endpoints</small>"]
    scheduler["Job scheduler<br/><small>Python/asyncio</small><br/><small>FIFO admission, exactly one active work item</small>"]
    bus["Event bus<br/><small>pyee AsyncIOEventEmitter</small><br/><small>Dispatches in-process commands and facts</small>"]
    youtubeProducer["YouTube producer<br/><small>yt-dlp adapter</small><br/><small>Expands playlists, produces local artifacts</small>"]
    telegramUploader["Telegram uploader<br/><small>Telethon adapter</small><br/><small>Consumes generic artifacts and uploads them</small>"]
    tracker["Lifecycle tracker<br/><small>In-memory projections</small><br/><small>Builds immutable job and batch snapshots</small>"]
    storage["Artifact storage<br/><small>FD-relative filesystem API</small><br/><small>Reserves, stages, validates, measures, deletes</small>"]
    cleanup["Artifact cleanup<br/><small>Event listener</small><br/><small>Deletes job directories after terminal events</small>"]
    lifecycle["Application lifecycle<br/><small>FastAPI lifespan</small><br/><small>Builds components — readiness, rollback, shutdown</small>"]
  end

  api -->|"Submits/reserves work"| scheduler
  api -->|"Reads projections"| tracker
  api -->|"Streams multipart data"| storage
  scheduler -->|"Emits commands and lifecycle facts"| bus
  bus -->|"Dispatches YouTube commands"| youtubeProducer
  youtubeProducer -->|"Allocates and validates downloads"| storage
  youtubeProducer -->|"Emits artifact or production failure"| bus
  bus -->|"Dispatches artifact.ready"| telegramUploader
  telegramUploader -->|"Validates artifact"| storage
  telegramUploader -->|"Emits uploaded, failed, or unavailable"| bus
  bus -->|"Projects all lifecycle facts"| tracker
  bus -->|"Dispatches terminal upload facts"| cleanup
  cleanup -->|"Deletes terminal job directory"| storage
  lifecycle -->|"Registers listeners and global error handling"| bus

  classDef comp fill:#4a86e8,stroke:#2a66c8,color:#fff
  class api,scheduler,bus,youtubeProducer,telegramUploader,tracker,storage,cleanup,lifecycle comp
```

## Event choreography

### YouTube video

```mermaid
sequenceDiagram
  participant Client
  participant API
  participant Scheduler
  participant Bus
  participant YouTube
  participant Telegram
  participant Tracker
  participant Cleanup

  Client->>API: POST /jobs/youtube
  API->>Scheduler: submit_video(url)
  Scheduler->>Bus: job.queued
  Scheduler-->>API: JobRef
  API-->>Client: 202 Accepted
  Scheduler->>Bus: job.started
  Scheduler->>Bus: youtube.download.requested
  Bus->>YouTube: async handler
  YouTube->>Bus: artifact.ready
  Bus->>Telegram: async handler
  Telegram->>Bus: artifact.uploaded or artifact.upload.failed
  Bus->>Tracker: update projection
  Bus->>Cleanup: delete temporary job directory
  Bus->>Scheduler: release active slot and pump next job
```

### Playlist

- Playlist submission creates a batch and occupies the active scheduler slot during expansion.
- YouTube emits ordered targets plus skipped-entry count.
- Scheduler deduplicates targets, creates one child job per target, and inserts children at queue front.
- Later standalone submissions wait until playlist children finish.
- Batch status derives from child projections: completed, failed, or partially completed.

### Direct upload

- API reserves job/artifact IDs and a safe destination.
- Multipart bytes are bounded during parsing, then streamed into artifact storage.
- Scheduler queues the staged artifact only after staging succeeds.
- When its FIFO turn arrives, scheduler emits `artifact.ready`; Telegram remains source-agnostic.

## Event contracts

Commands:

- `youtube.playlist.expansion.requested`
- `youtube.download.requested`

Facts:

- `batch.created`
- `batch.jobs.created`
- `job.queued`
- `job.started`
- `youtube.playlist.expanded`
- `youtube.playlist.expansion.failed`
- `artifact.ready`
- `artifact.production.failed`
- `artifact.uploaded`
- `artifact.upload.failed`
- `telegram.unavailable`

All event payloads are frozen domain objects with timezone-aware timestamps. Producers and consumers depend only on shared contracts, not on each other.

## Runtime and lifecycle

Startup order:

1. Load settings from the application base directory.
2. Validate/claim artifact storage and clear startup orphans.
3. Create one event bus and register global/error listeners.
4. Register tracker, scheduler, cleanup, YouTube, and Telegram components.
5. Connect Telegram and start its disconnect monitor.
6. Open readiness and accept submissions.

Shutdown order:

1. Close readiness and scheduler admission.
2. Stop accepting new Telegram artifacts.
3. Drain active event tasks within configured grace.
4. Cancel remaining work and await cleanup.
5. Stop uploader/monitor.
6. Clear artifact directories best-effort.
7. Disconnect Telegram last.

## Failure boundaries

- Scheduler owns queue state; tracker only observes facts.
- Listener/programming failures mark scheduler fatal and readiness false.
- Telegram disconnection fails the current upload, emits `telegram.unavailable`, pauses scheduling, and rejects new submissions.
- yt-dlp runs in its own process group; timeout/cancellation sends terminate, then kill if still alive.
- Known and unknown YouTube sizes are bounded using `--max-filesize` plus FD-relative directory-size monitoring.
- Terminal upload events trigger idempotent artifact cleanup.
- All queues, projections, dedupe registries, and events are process-local and non-durable.

## HTTP surface

- `POST /jobs/youtube`
- `POST /jobs/upload`
- `GET /jobs/{job_id}`
- `GET /batches/{batch_id}`
- `GET /health`

The intended launch target is `anything2telegram.main:app`, bound locally to `127.0.0.1`.
