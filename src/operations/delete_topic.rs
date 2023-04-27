use rust_extensions::date_time::DateTimeAsMicroseconds;
use serde::*;

use crate::app::AppContext;

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

    app.topics_list.delete(topic_id).await;

    let message_id = message_id.unwrap();

    app.topics_snapshot
        .add_deleted_topic(topic_id, message_id, delete_after)
        .await;
}
