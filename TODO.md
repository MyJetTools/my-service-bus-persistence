## TODO (detailed)

- HTTP delete parity
  - `/api/Topic` DELETE currently removes the topic from snapshot only. Wire it to the same soft-delete flow as gRPC: call `operations::delete_topic`, add to `deleted_topics` with `gc_after`, and avoid orphaned blobs. Keep secret-key check intact.

- gRPC delete robustness
  - `operations::delete_topic` panics when topic missing, crashing the server via gRPC delete. Return a tonic `not_found` (or similar) instead so clients get an error without taking down the node.

- History-by-date implementation
  - `GetHistoryByDate` gRPC now returns an empty stream. Implement retrieval by timestamp range (likely leveraging index-by-minute) so clients can consume historical messages.

- Settings tilde expansion
  - Only `messages_connection_string` expands `~`. Apply the same expansion to `topics_connection_string` and `archive_connection_string` for consistent config handling.
