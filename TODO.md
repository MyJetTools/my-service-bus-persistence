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
   stored data (archives, indexes, the open tail) stays intact on
   disk. The soft-delete record is persisted in the topics snapshot so
   it survives a restart.
2. **Hard delete (GC)** — a background timer periodically scans the list
   of soft-deleted topics. Any whose `gc_after` is in the past gets its
   data permanently removed (delete `{namespace}/{topic}/` and the
   matching cold-storage keys, drop the in-memory `TopicData`, remove
   the soft-delete record). Until that
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

The topics-snapshot persisted format already has a `deleted_topics`
field (`DeletedTopicProtobufModel { topic_id, message_id, gc_after,
namespace }`), so on-disk compatibility with previously soft-deleted
topics is preserved. The record is namespace-aware, so GC can never
delete a same-named topic in another namespace.

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
  the error on delete failure but had already removed the soft-delete
  record beforehand, so a transient storage error meant the data was
  orphaned forever. The flow should remove the soft-delete record only
  after the storage delete actually succeeds (or be idempotent and
  re-attempt safely).

---

## Other open items

- History-by-date implementation
  - `GetHistoryByDate` gRPC now returns an empty stream. Implement
    retrieval by timestamp range (likely leveraging index-by-minute)
    so clients can consume historical messages.

---

## Storage: files + S3

Implemented. Azure is gone - no `my-azure-*` crate is left in
[Cargo.toml](Cargo.toml) - and everything is written straight to files, with S3
as an optional cold tier.

### What landed

```text
{data_folder}/
    .layout-version               marks the folder as laid out by namespace
    {namespace}/
        topics-and-queue.yaml     that namespace's topics + queues, human readable
        {topic}/
            {:019}.archive        sealed sub pages: TOC + compressed blocks
            .{year}.yearindex     527 040 minutes x 8 bytes, addressed at minute*8
            active                the open tail - the sub page still being filled
```

- `{namespace}/{topic}/...` doubles as the S3 key; `app/storage_layout.rs` is the
  only place that spells either of them.
- `default` is not special. Pre-namespace folders sitting at the root are renamed
  into `default/` on first start (`operations::migrate_legacy_topics`) - a move
  within one mount, nothing copied. The marker is `{data_folder}/.layout-version`,
  and its *absence* is what matters: before namespaces the concept did not exist,
  so every folder at the root is a topic, with nothing to sniff or guess. The
  marker is written last, so a crash halfway is finished by the next start.
- One `data_folder` replaced the three connection strings, which also resolves the
  old tilde-expansion inconsistency.
- The uploader timer (60 s) keeps the highest-numbered archive and year index per
  topic and sends up all the rest - a backlog drains in one tick. No state is
  persisted: "highest on disk is current" holds by itself across rollovers and
  restarts. Upload, then delete locally; a crash in between costs a repeated PUT,
  which is idempotent.
- Reads look local first, then cold. A cold archive is never downloaded whole: the
  TOC is fetched once and cached forever (the object is immutable), then one ranged
  GET per sub page. A cold year index is instead pulled back to disk in full - 4 MB,
  addressed by offset - which makes a late write for a closed year work with no
  special case, and the uploader sends the updated copy back up.

### Audits

Two adversarial passes have run over this code. The first produced 20 findings, all 20 verified by
refutation, 8 confirmed and fixed:

- the year index was never registered on the write path, so nothing was ever indexed (4 findings,
  one defect);
- a resumed migration nested `default/` under `default/default`;
- a topic id containing `/` or `..` escaped its namespace folder - topic ids are now validated at
  every entry point, and `get_local_path` refuses an escaping component as a last line of defence;
- archive handles were cached in two independent lists, so a second `FileStorage` could be handed
  out for a file another writer already held - there is one cache now;
- a successful S3 DELETE (204) was read as a failure.

A second pass over the migration, snapshot and settings code produced 14 findings, all verified,
2 confirmed and fixed:

- the legacy `topicsdata` was imported whenever a `legacy` section was configured but converted
  only when `.layout-version` was absent, so starting once without the section and adding it
  afterwards stranded the whole topic list in a file nothing reads. The conversion is now keyed on
  the file being present, and merges rather than overwrites - the live snapshot is the newer one;
- `restore_legacy` deleted `.active-pages` even when it could not decode it. It holds the open tail
  of every topic and is the only copy by then; an undecodable one is now left in place, matching
  what the per-topic path already did.

Both migration bugs were found by *running* the service, not by the tests: each function behaved
correctly on its own and only their composition was wrong.

### Still open

- **`active`: dump on shutdown, or an append-only journal?** It is still a dump,
  written in `before_shut_down`, so only a graceful restart is survived: `kill -9`,
  OOM or power loss lose the tail. And the tail is unbounded in time, not just in
  size - a topic that goes quiet mid-sub-page keeps those messages in RAM until
  shutdown, because GC only evicts a sub page once a newer one exists. On a plain
  file the tail could be appended to as messages arrive, shrinking the loss window
  to the last fsync and removing the shutdown path entirely. It changes the file
  format, so it is a deliberate decision rather than a refactor.
- **`my-s3`: a typed `KeyNotFound`** instead of `Other("Status Code: 404...")`, which
  `cold_storage::is_not_found` has to match by string today. `If-Match` on PUT is not
  needed while a topic has a single writer, and listing is not needed at all.
- **`ARCHIVE_MESSAGES_PER_FILE`** (10M) drives the local disk peak before upload and
  should become a setting.
- **Nothing has run against a real AWS/MinIO endpoint yet.** The client is exercised against an
  in-process S3-compatible server (`cold_storage::fake_s3`), which covers SigV4 signing, the
  `Range` header, 200/206/204/404 handling, the key spelling and the cold archive read - but not
  a real provider's quirks. One smoke test against the actual bucket is still worth doing.
- **No automated end-to-end test.** The storage primitives, the layout and the migration are
  covered by 55 unit tests, and the whole flow has been driven by hand against a live process
  (migration, two namespaces, archive, restart, year index, per-namespace YAML). Nothing runs it
  automatically, and `cargo test` is still commented out in CI.
- **`PagesGcTimer` is driven by the topics snapshot, not by memory.** `gc_pages` iterates
  `topics_snapshot.snapshot.data`, so a topic that has received messages but is not in a saved
  snapshot never has its sub pages evicted or archived - they accumulate in RAM. The bus node
  pushes a snapshot every couple of seconds, so in practice the window is seconds; but the same
  shape already caused a real bug in `restore()` and is worth making memory-driven, using the
  snapshot entry only for the `persist` flag.
- **`my-s3` answers a successful DELETE with 204**, which the crate treats as an error, so every
  delete came back as `Other("Status Code: 204...")` and hard delete removed nothing from the cold
  tier. Worked around in `cold_storage::is_no_content` by matching the rendered status code -
  replace it once the crate handles 204 (and gives a typed `KeyNotFound`).
