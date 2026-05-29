# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this service is

`my-sb-persistence` is the durable-storage node for [`my-service-bus`](https://github.com/MyJetTools/my-service-bus).
The main bus node is the **only** canonical client: it calls this service over gRPC (port `7124`) to persist topics,
queue snapshots and messages into Azure Page Blobs (or local file-backed page-blob emulation for dev). Do not call
this service directly from application code.

The README ([README.md](README.md)) is the authoritative operator-facing reference: settings, endpoints, ports,
storage layout, lifecycle/timers. Read it before changing config, endpoints, or the blob layout.

## Build / run / test

```sh
cargo check                 # fast feedback loop (preferred while iterating)
cargo build --release        # what CI builds; Docker image consumes target/release/my-sb-persistence
cargo run --release          # local run; reads YAML config from $HOME/.myservicebus-persistence
```

- There are currently **no Rust unit/integration tests** (`cargo test` is commented out in CI).
- `build.rs` downloads `MyServicePersistenceGrpcService.proto` from the `my-sb-proto-files` GitHub repo at
  build time (via `ci-utils::sync_and_build_proto_file`) and compiles it into the `crate::persistence_grpc`
  module (`tonic::include_proto!("persistence")`). The local `proto/` copy is a synced artifact — the proto
  contract lives in the remote repo, so edit it there, not here.
- Requires `$HOME` to be set: the settings filename is resolved relative to it. jemalloc is the global allocator.

## MyJetTools dependencies (important)

Nearly every dependency is a MyJetTools (`MyJetTools/*`) git crate pinned by tag in [Cargo.toml](Cargo.toml):
`my-service-bus-sdk`, `my-azure-storage-sdk`, `my-azure-page-blob-*`, `my-http-server`, `my-grpc-extensions`,
`rust-extensions`, `my-logger`. These evolve constantly — **never rely on memory for their API signatures.**
Per the global rules, consult the `development-best-practices` MCP resources first (e.g. `my-grpc-extensions`,
`rust-extensions`, `my-postgres`, `performance-considerations`) before writing code against them.

## Architecture

`main.rs` builds a single `Arc<AppContext>` (shared, read-mostly state) and wires up:
- gRPC server on `:7124` (+ optional Unix socket from `listen_unix_socket`).
- HTTP server on `:7123` (health/status, reads, Prometheus, static UI from `./wwwroot`).
- `data_initializer::init` — restores active sub-pages from the topics snapshot; until it finishes,
  `app.app_states` is *un-initialized* and gRPC/HTTP reject requests via `contracts::check_flags`.
- Background timers (`rust_extensions::MyTimer`): 3 s (topics-snapshot saver, min-index saver),
  1 s (pages GC, metrics updater). The 30 s `DeletedTopicsGc` timer is disabled (see TODO below).
- Graceful shutdown via `before_shut_down::execute_before_shutdown` (flush yearly index, archive in-flight
  sub-pages, persist topics snapshot).

### Module map (`src/`)
- `app/` — `AppContext` (global state, connection strings, blob factories), `file_name_generators`, Prometheus metrics.
- `grpc/` — tonic service impl (`persistence_grpc_service.rs`), server bootstrap, request/response mappers and contracts.
- `http/` — `my-http-server` controllers (api/home/logs/prometheus/read/topic), builder, start_up.
- `topics_snapshot/` — the topics+queues snapshot (protobuf `TopicsSnapshotProtobufModelV2`), persisted to the
  `topics/topicsdata` page blob. Source of truth for which topics exist.
- `topic_data/` — in-memory per-topic state. `TopicsDataList` is the registry; `TopicData` holds `PagesList`,
  yearly index, archive list, metrics.
- `message_pages/` — message storage in **sub-pages** (`SubPageId` derived from `MessageId`). `SubPage` is an enum:
  `Active` (mutable, in-memory under a `parking_lot::Mutex`), `FromArchive` (read-only restored), `Missing`.
- `archive_storage/` — rotated archive page blobs per topic, indexed by `ArchiveFileNo` with a TOC.
- `index_by_minute/` — per-topic per-year minute index that powers time-range reads (`ListFromDate`).
- `operations/` — the business-logic layer (read/write/GC/init/shutdown flows). gRPC and HTTP handlers should
  delegate here rather than touching storage structures directly.
- `timers/` — the registered `MyTimer` jobs.
- `settings.rs` — `SettingsModel` (YAML at `$HOME/.myservicebus-persistence`, read once at startup).

### Concurrency conventions

The coding/concurrency conventions are **not duplicated here** — they live in the `development-best-practices`
MCP (`performance-considerations`, `application-architecture-best-practices`, `rust-extensions`). Read those
before writing code; the global rules require it. This codebase follows them: e.g. `TopicsDataList` uses
`ArcSwap` + copy-on-write, and storage structs lock with `parking_lot` while handing out `Arc` snapshots.

## TODO / disabled features (read before touching delete/restore)

The **soft-delete topic + GC** flow is intentionally disabled and partially commented out across the codebase
(grep `// TODO:`). The on-disk snapshot format keeps `deleted_topics` for compatibility. The gRPC
`DeleteTopic`/`RestoreTopic` were `unimplemented`; note `HardDeleteTopic` gRPC has since been added (see recent
commits). `GetHistoryByDate` returns an empty stream. Before reworking any of this, read [TODO.md](TODO.md) —
it documents the intended two-stage design and the specific hazards to avoid (don't `panic!` on deleted-topic
access; only drop the soft-delete record after storage delete succeeds).

## Release

Two CI paths exist: `.github/workflows/` (GitHub Actions) and `.gitlab-ci.yml`. Both build the release binary and
publish a Docker image tagged from the `Cargo.toml` version (`amigin/my-sb-persistence:<version>`). The Dockerfile
copies the prebuilt `target/release/my-sb-persistence` and `./wwwroot` — build the binary before building the image.
See the `release-guide` MCP resource for the standard release/deploy flow.
