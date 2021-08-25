use crate::{app::AppContext, toipics_snapshot::TopicsDataProtobufModel};

pub async fn execute(app: &AppContext, topics: &TopicsDataProtobufModel) {
    for topic in &topics.data {
        let topic = app.get_data_by_topic(topic.topic_id.as_str()).await;

        if topic.is_none() {
            continue;
        }

        let topic = topic.unwrap();

        let queue_size = topic.get_queue_size().await;

        app.metrics_keeper
            .update_topic_queue_size(topic.topic_id.as_str(), queue_size)
            .await;
    }
}
