use std::sync::Arc;

use crate::{app::AppContext, topic_data::TopicData};

pub async fn get_topic_data_to_publish_messages(
    app: &AppContext,
    topic_id: &str,
) -> Arc<TopicData> {
    if let Some(topic_data) = app.topics_list.get(topic_id).await {
        return topic_data;
    }

    let message_id = app.topics_snapshot.get_current_message_id(topic_id).await;

    if message_id.is_none() {
        panic!("There is not topic {} in snapshot", topic_id);
    }

    super::init_new_topic(app, topic_id, message_id.unwrap()).await
}
