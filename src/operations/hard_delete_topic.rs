use std::sync::Arc;

use my_azure_storage_sdk::{blob_container::BlobContainersApi, AzureStorageError};
use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::{app::AppContext, topics_snapshot::DeletedTopicProtobufModel};

use super::OperationError;

async fn delete_containers(
    topic_id: &str,
    connections: &[Arc<my_azure_storage_sdk::AzureStorageConnection>],
) -> Result<(), AzureStorageError> {
    for conn in connections {
        // Best-effort delete; if the container is already gone, we keep going.
        if let Err(err) = conn.delete_container_if_exists(topic_id).await {
            return Err(err);
        }
    }

    Ok(())
}

pub async fn hard_delete_topic(
    app: &AppContext,
    deleted_topic: &DeletedTopicProtobufModel,
) -> Result<(), OperationError> {
    // Remove from deleted list first to avoid repeated attempts on failure loops
    let removed = app
        .topics_snapshot
        .remove_deleted_topic(deleted_topic.topic_id.as_str())
        .await;

    if removed.is_none() {
        return Ok(()); // Already removed by a concurrent worker
    }

    // Remove any cached topic data
    app.topics_list
        .remove(deleted_topic.topic_id.as_str())
        .await;

    // Attempt to delete storage containers
    delete_containers(
        deleted_topic.topic_id.as_str(),
        &[
            app.get_topics_conn(),
            app.get_messages_conn(),
            app.get_archive_conn(),
        ],
    )
    .await
    .map_err(|err| OperationError::AzureStorageError(err))?;

    Ok(())
}

pub async fn gc_expired_deleted_topics(app: &AppContext) {
    let now = DateTimeAsMicroseconds::now().unix_microseconds;
    let snapshot = app.topics_snapshot.get().await;

    for deleted in snapshot.snapshot.deleted_topics {
        if deleted.gc_after <= now {
            if let Err(err) = hard_delete_topic(app, &deleted).await {
                my_logger::LOGGER.write_error(
                    "GcDeletedTopic".to_string(),
                    format!(
                        "Failed to delete topic {} permanently: {:?}",
                        deleted.topic_id, err
                    ),
                    my_logger::LogEventCtx::new(),
                );
            }
        }
    }
}
