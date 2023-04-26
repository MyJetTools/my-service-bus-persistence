use std::sync::Arc;

use crate::{app::AppContext, topic_data::TopicData};

pub async fn init_new_topic(app: &AppContext, topic_id: &str) -> Option<Arc<TopicData>> {
    if app.topics_list.create_topic_data(topic_id).await {
        app.create_topic_container(topic_id).await;
    }

    app.topics_list.get(topic_id).await
}
