# MY SERVICE BUS PERSISTENCE

## Run  

Enusure that environment variable "**HOME**" exists.
It should point to location with **.myservicebus-persistence** file!

**.myservicebus-persistence** content:
```
QueuesConnectionString: <Connection string to azure storage account>
MessagesConnectionString: <Connection string to azure storage account>
LoadBlobPagesSize: 8192
FlushQueuesSnapshotFreq: 00:00:01
FlushMessagesFreq: 00:00:01
MaxResponseRecordsAmount: 500
DeleteTopicSecretKey: SecretKeyString
```
Install rust: https://www.rust-lang.org/tools/install
execute: **cargo run --release**

## What this service does

- Companion persistence node for [`my-service-bus`](https://github.com/MyJetTools/my-service-bus); main node calls this over gRPC for durable storage.
- Persists topics/queues/messages in Azure Page Blobs, keeps topic snapshots, and exposes HTTP for health/reads/metrics.

## Architecture at a glance

- App context sets up Azure connections for topics/messages/archive, loads topic snapshot blob, and keeps per-topic in-memory state.
- Messages are grouped by subpage (derived from message id), written into page blobs, and indexed by minute for time-based reads.
- Topic snapshots (topics + queues) are stored in `topics/topicsdata` page blob and cached in memory.
- Archive rotation uses dedicated archive page blobs per topic (see `archive_storage`).

## Interfaces

gRPC (7124):
- Queue snapshot stream get/save; message/page/subpage reads (compressed or plain); save messages (stream); delete/restore topic; version/ping.

HTTP (7123):
- `/api/is_alive`, `/api/status` for health/status.
- `/Read/ById`, `/Read/ListFromDate` to fetch messages (JSON, base64 payload).
- `/api/Topic` DELETE (requires secret) and GET (deleted topics list).
- `/metrics` Prometheus exposition.
- Serves static UI from `wwwroot` with Swagger docs enabled.

## Timers & lifecycle

- 3s timers: topic snapshot saver, min-index saver.
- 1s timers: page GC, metrics updater.
- `app_states` gates traffic: gRPC returns “not initialized/shutting down” until ready; graceful shutdown runs before-exit hook.

## Storage layout & indexing

- Pages/subpages: message ids map to subpages; writes group by subpage, update minute index, and append to blob-backed subpages.
- Compressed page compiler builds chunked responses (4 MiB cap) for efficient gRPC `GetPageCompressed`.
- Index-by-minute blobs per topic-year speed `ListFromDate` queries.
- Deleted topics are tracked in snapshot; restore can rehydrate topic id and last message id.

## Metrics

- `/metrics` includes topic/page/subpage sizes, topic counts, and HTTP connection counts for Prometheus scraping.
