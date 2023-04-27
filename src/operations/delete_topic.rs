use my_azure_storage_sdk::block_blob::BlockBlobApi;
use rust_extensions::date_time::DateTimeAsMicroseconds;
use serde::*;

use crate::app::{file_name_generators::SOFT_DELETE_METADATA_FILE_NAME, AppContext};

#[derive(Serialize, Deserialize, Debug)]
pub struct TopicSoftDeleteMetadataBlobModel {
    pub message_id: i64,
    pub delete_after: String,
}

pub async fn delete_topic(app: &AppContext, topic_id: &str, delete_after: DateTimeAsMicroseconds) {
    let message_id = app.topics_snapshot.get_current_message_id(topic_id).await;

    if message_id.is_none() {
        panic!("Topic {} not found", topic_id)
    }

    let message_id = message_id.unwrap();

    let topic_soft_delete_model = TopicSoftDeleteMetadataBlobModel {
        message_id: message_id.get_value(),
        delete_after: delete_after.to_rfc3339(),
    };

    let payload = serde_json::to_vec(&topic_soft_delete_model).unwrap();

    let azure_storage_connection = app.settings.get_messages_azure_storage_connection();

    azure_storage_connection
        .upload_block_blob(topic_id, SOFT_DELETE_METADATA_FILE_NAME, payload)
        .await
        .unwrap();

    app.topics_list.delete(topic_id).await;
}
