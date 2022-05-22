use std::sync::Arc;

use my_service_bus_shared::MessageId;

use crate::{app::AppContext, topic_data::TopicData};

pub async fn init_new_topic(
    app: &AppContext,
    topic_id: &str,
    max_message_id: MessageId,
) -> Arc<TopicData> {
    if app
        .topics_list
        .create_topic_data(topic_id, max_message_id)
        .await
    {
        app.create_topic_folder(topic_id).await;
    }

    app.topics_list.get(topic_id).await.unwrap()
}
