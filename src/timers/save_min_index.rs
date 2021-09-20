use std::sync::Arc;

use crate::{app::AppContext, toipics_snapshot::TopicsDataProtobufModel};

pub async fn execute(app: Arc<AppContext>, topics: Arc<TopicsDataProtobufModel>) {
    let timer_result = tokio::spawn(timer_tick(app.clone(), topics)).await;

    if let Err(err) = timer_result {
        app.logs.add_fatal_error("save_min_index_timer", err).await;
    }
}

async fn timer_tick(app: Arc<AppContext>, topics: Arc<TopicsDataProtobufModel>) {
    for topic in &topics.data {
        let index_handler = app.index_by_minute.get(topic.topic_id.as_str()).await;

        index_handler
            .save_to_storage(&app.index_by_minute_utils)
            .await;
    }
}
