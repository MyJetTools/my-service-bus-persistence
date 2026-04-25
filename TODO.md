# TODO

## Soft-delete topic + scheduled GC

The topic delete flow is currently disabled. The previous in-progress
implementation has been commented out (see `// TODO:` markers across the
codebase) until the design below is implemented.

### Goal

Deleting a topic must not immediately destroy its data. Instead it is a
two-stage operation:

1. **SoftDelete** — the user marks a topic for deletion together with an
   expiration timestamp (`gc_after`). The topic is removed from the
   "active" topics list so producers/consumers stop seeing it, but its
   stored data (page blobs, archive blobs, indexes) stays intact on
   disk. The soft-delete record is persisted in the topics snapshot so
   it survives a restart.
2. **Hard delete (GC)** — a background timer periodically scans the list
   of soft-deleted topics. Any whose `gc_after` is in the past gets its
   data permanently removed (delete the blob containers, drop the
   in-memory `TopicData`, remove the soft-delete record). Until that
   moment a soft-deleted topic can also be **restored** (the soft-delete
   record is dropped and the topic becomes active again).

### Where the disabled code lives

- `src/operations/delete_topic.rs` — soft-delete operation
  (`delete_topic`) plus `TopicSoftDeleteMetadataBlobModel`.
- `src/operations/hard_delete_topic.rs` — GC operation
  (`hard_delete_topic`, `gc_expired_deleted_topics`).
- `src/operations/restore_topic.rs` — restore operation.
- `src/operations/mod.rs` — module declarations / re-exports.
- `src/timers/deleted_topics_gc.rs` + `src/timers/mod.rs` —
  `DeletedTopicsGcTimer` (30 s tick) that drives the GC.
- `src/main.rs` — registration of the GC timer.
- `src/topics_snapshot/current_snapshot.rs` — `add_deleted_topic`,
  `remove_deleted_topic` on `TopicsSnapshotData` /
  `CurrentTopicsSnapshot`.
- `src/topic_data/topics_data_list.rs` — `delete()` (Arc-swapped CoW
  variant; previously also kept a `deleted: HashSet<String>` to panic
  on later access — that mechanism should NOT be reintroduced as-is,
  see "Things to reconsider" below).
- `src/grpc/persistence_grpc_service.rs` — `delete_topic`,
  `restore_topic` gRPC methods (currently return
  `Status::unimplemented`).
- `src/http/controllers/topic_controller/delete_topic_action.rs` and
  `get_deleted_action.rs` — HTTP `DELETE /api/Topic` and
  `GET /api/Topic`.
- `src/http/controllers/topic_controller/mod.rs` — submodules.
- `src/http/builder.rs` — controller registration.
- `src/http/controllers/topic_controller/contracts.rs` —
  `DeleteTopicHttpContract` (kept with `#[allow(dead_code)]`).
- `src/app/app_ctx.rs` — `get_topics_conn` / `get_messages_conn` /
  `get_archive_conn` (used only by `hard_delete_topic`; kept with
  `#[allow(dead_code)]`).

The topics-snapshot persisted format already has a `deleted_topics`
field (`DeletedTopicProtobufModel { topic_id, message_id, gc_after }`),
so on-disk compatibility with previously soft-deleted topics is
preserved.

### Things to reconsider when re-enabling

- **Don't `panic!` on access to a deleted topic.** The previous
  `topics_data_list.get()`/`create_topic_data()` panicked when the topic
  id was in a `deleted: HashSet<String>`. That set was only ever
  *appended to*, never cleared by `hard_delete_topic`, so a topic with
  the same name could never be recreated. Either drop the set entirely
  (the snapshot is the source of truth) or make sure `hard_delete_topic`
  clears the entry, and never panic — return `None` / a typed error
  instead.
- **Restore vs. recreate race.** Decide what happens if a producer
  publishes to a topic that is currently soft-deleted: reject, auto-
  restore, or auto-create new? Currently this is undefined.
- **`delete_topic_secret_key`.** The HTTP handler used a settings-level
  shared secret. Confirm whether the gRPC handler should require a
  similar guard.
- **`hard_delete_topic` error handling.** The previous version logged
  the error on container delete failure but had already removed the
  soft-delete record beforehand, so a transient Azure error meant the
  data was orphaned forever. The flow should remove the soft-delete
  record only after the storage delete actually succeeds (or be
  idempotent and re-attempt safely).

---

## Other open items

- History-by-date implementation
  - `GetHistoryByDate` gRPC now returns an empty stream. Implement
    retrieval by timestamp range (likely leveraging index-by-minute)
    so clients can consume historical messages.

- Settings tilde expansion
  - Only `messages_connection_string` expands `~`. Apply the same
    expansion to `topics_connection_string` and
    `archive_connection_string` for consistent config handling.
