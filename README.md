# my-service-bus-persistence

Companion persistence node for [`my-service-bus`](https://github.com/MyJetTools/my-service-bus).
The main node calls this service over gRPC for durable storage of
topics, queues and messages, kept in Azure Page Blobs (or local
emulated page-blob files for development).

## Run

### 1. Install Rust

https://www.rust-lang.org/tools/install

### 2. Make sure `HOME` is set

The service reads its configuration file from
`$HOME/.myservicebus-persistence` on Linux/macOS and
`%HOME%\.myservicebus-persistence` on Windows. This is resolved at
compile time via `env!("HOME")`, so the variable must be present in
the environment of whoever runs the binary.

### 3. Create the config file

`$HOME/.myservicebus-persistence` is a YAML file. Minimal example:

```yaml
topics_connection_string: "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"
messages_connection_string: "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"
archive_connection_string: "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"
max_response_records_amount: 500
delete_topic_secret_key: "some-shared-secret"
# Optional, omit if you don't want a Unix-socket gRPC endpoint:
# listen_unix_socket: "/tmp/my-sb-persistence.sock"
```

For local development you can point any of the connection strings to
a local file-system emulator instead of Azure (e.g.
`AccountName=devstoreaccount1;...`, or a `~/path` for the file-backed
implementation — see `messages_connection_string` note below).

### 4. Build & run

```sh
cargo run --release
```

Or run a pre-built binary directly:

```sh
./target/release/my-sb-persistence
```

The Docker image expects the binary at
`./target/release/my-sb-persistence` and a `./wwwroot` directory next
to it (see `Dockerfile`). Build the binary first
(`cargo build --release`) and then build the image.

## Settings reference

All fields live in `$HOME/.myservicebus-persistence` (YAML) and map
1-to-1 to `SettingsModel` in [`src/settings.rs`](src/settings.rs).

| Field                          | Type             | Required | Description                                                                                                                                          |
| ------------------------------ | ---------------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `topics_connection_string`     | `string`         | yes      | Azure Storage connection string used for the topics-snapshot blob (`topics/topicsdata`).                                                             |
| `messages_connection_string`   | `string`         | yes      | Azure Storage connection string used for messages page blobs and active sub-page state. A leading `~` is expanded to `$HOME` (file-backed dev mode). |
| `archive_connection_string`    | `string`         | yes      | Azure Storage connection string used for archive page blobs (rotated history per topic).                                                             |
| `max_response_records_amount`  | `usize`          | yes      | Upper bound on records returned per HTTP read response.                                                                                              |
| `delete_topic_secret_key`      | `string`         | yes      | Shared secret for the HTTP `DELETE /api/Topic` endpoint. Currently unused while soft-delete + GC is being reworked — see [TODO.md](TODO.md).         |
| `listen_unix_socket`           | `string` (opt.)  | no       | If set, gRPC additionally listens on this Unix socket path (in addition to TCP `:7124`). Useful for sidecar deployments.                             |

Notes:
- Tilde (`~`) expansion is currently only applied to
  `messages_connection_string`. `topics_connection_string` and
  `archive_connection_string` do not expand `~` — see [TODO.md](TODO.md).
- The settings file is read once at startup; changing it requires a
  process restart.

## Network endpoints

| Port    | Protocol | Purpose                                                                |
| ------- | -------- | ---------------------------------------------------------------------- |
| `7123`  | HTTP     | Health, status, reads, Prometheus metrics, static UI / Swagger.        |
| `7124`  | gRPC     | Persistence API consumed by the `my-service-bus` main node.            |
| `listen_unix_socket` | gRPC | Optional Unix-socket variant of the gRPC API.                       |

### HTTP endpoints (port 7123)

- `GET /api/IsAlive` — liveness probe.
- `GET /api/Status` — runtime status (initialization flag, queue
  snapshot id, per-topic loaded pages, system memory).
- `GET /Read/ById?...` — fetch a single message by id (JSON, payload
  is base64-encoded).
- `GET /Read/ListFromDate?...` — fetch messages by time range
  (JSON, base64 payload). Backed by per-year minute index.
- `GET /metrics` — Prometheus exposition.
- Static UI under `/` is served from `./wwwroot`. Swagger is
  available for the registered controllers.
- HTTP `DELETE /api/Topic` and `GET /api/Topic` (deleted topics
  list) are temporarily disabled — see [TODO.md](TODO.md).

### gRPC endpoints (port 7124)

Defined by the `persistence.proto` contract (compiled into
`crate::persistence_grpc`). The service exposes:

- `GetVersion`, `Ping`.
- Queue snapshot stream get/save.
- Message / page / sub-page reads (compressed and plain variants).
- `SaveMessages` (client-streaming).
- `GetHistoryByDate` — currently returns an empty stream (TODO).
- `DeleteTopic` / `RestoreTopic` — currently return
  `Status::unimplemented` while soft-delete + GC is being reworked
  (see [TODO.md](TODO.md)).

`my-service-bus` main node is the canonical client; do not call this
service directly from application code.

## Lifecycle & timers

- On startup `data_initializer` restores active sub-pages from the
  topics-snapshot blob; gRPC requests respond with
  `Initializing` until that finishes.
- Background timers:
  - 3 s tick — topic-snapshot saver, min-index saver.
  - 1 s tick — page GC, metrics updater.
- Graceful shutdown runs `before_shut_down` to flush yearly
  index, archive in-flight sub-pages, and persist the topics
  snapshot before the process exits.

## Storage layout

- Topics-snapshot blob: `topics/topicsdata` (page blob in
  `topics_connection_string`).
- Messages: page blobs per topic in `messages_connection_string`,
  organised by sub-page (derived from `MessageId` →
  `SubPageId`).
- Archive: rotated page blobs per topic in
  `archive_connection_string`, indexed by `ArchiveFileNo`.
- Per-topic per-year minute index drives time-based reads.

## Development

- `cargo check` — fast feedback loop.
- `cargo run --release` — local run with the YAML config from
  `$HOME`.
- The repo expects a Tokio multi-threaded runtime (default
  `#[tokio::main]`) and uses jemalloc as the global allocator.

## Project conventions

- Performance / concurrency rules: see
  [performance-considerations.md](performance-considerations.md)
  (ArcSwap for read-mostly state, `parking_lot` over `tokio::sync`
  whenever there is no `.await` under the guard, AHash instead of
  `std::HashMap`/`HashSet`, no heavy CPU work under a Mutex, etc.).
- Outstanding work: [TODO.md](TODO.md).
