use std::sync::Arc;

use my_azure_storage_sdk::{blob_container::BlobContainersApi, AzureStorageConnection};

use crate::app::AppContext;

use super::OperationError;

async fn delete_container_best_effort(
    topic_id: &str,
    conn: &Arc<AzureStorageConnection>,
) -> Result<(), OperationError> {
    match conn.delete_container_if_exists(topic_id).await {
        Ok(_) => Ok(()),
        Err(err) => Err(OperationError::AzureStorageError(err)),
    }
}

pub async fn hard_delete_topic(app: &AppContext, topic_id: &str) -> Result<(), OperationError> {
    app.topics_list.remove(topic_id);

    delete_container_best_effort(topic_id, &app.get_topics_conn()).await?;
    delete_container_best_effort(topic_id, &app.get_messages_conn()).await?;
    delete_container_best_effort(topic_id, &app.get_archive_conn()).await?;

    Ok(())
}
