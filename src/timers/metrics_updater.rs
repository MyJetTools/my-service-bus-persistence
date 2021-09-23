use std::sync::Arc;

use my_service_bus_shared::protobuf_models::TopicsSnapshotProtobufModel;

use crate::app::AppContext;

pub async fn execute(app: Arc<AppContext>, topics: Arc<TopicsSnapshotProtobufModel>) {
    let timer_result = tokio::spawn(timer_tick(app.clone(), topics)).await;

    if let Err(err) = timer_result {
        app.logs.add_fatal_error("metrics_updater_timer", err).await;
    }
}

async fn timer_tick(app: Arc<AppContext>, topics: Arc<TopicsSnapshotProtobufModel>) {
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
