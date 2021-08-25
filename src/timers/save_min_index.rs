use crate::{app::AppContext, toipics_snapshot::TopicsDataProtobufModel};

pub async fn execute(app: &AppContext, topics: &TopicsDataProtobufModel) {
    for topic in &topics.data {
        let index_handler = app.index_by_minute.get(topic.topic_id.as_str()).await;

        index_handler
            .save_to_storage(&app.index_by_minute_utils)
            .await;
    }
}
