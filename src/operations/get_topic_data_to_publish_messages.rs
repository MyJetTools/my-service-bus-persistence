use std::sync::Arc;

use crate::{app::AppContext, topic_data::TopicData};

pub async fn get_topic_data_to_publish_messages(
    app: &AppContext,
    topic_id: &str,
) -> Arc<TopicData> {
    loop {
        if let Some(topic_data) = app.topics_list.get(topic_id).await {
            return topic_data;
        }

        super::init_new_topic(app, topic_id).await;
    }
}
