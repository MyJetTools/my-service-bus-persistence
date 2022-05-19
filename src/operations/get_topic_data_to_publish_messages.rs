use std::sync::Arc;

use my_service_bus_shared::MessageId;

use crate::{app::AppContext, topic_data::TopicData};

pub async fn get_topic_data_to_publish_messages(
    app: &AppContext,
    topic_id: &str,
    max_message_id: MessageId,
) -> Arc<TopicData> {
    loop {
        if let Some(topic_data) = app.topics_list.get(topic_id).await {
            return topic_data;
        }

        super::init_new_topic(app, topic_id, max_message_id).await;
    }
}
