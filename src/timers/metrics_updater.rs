use std::sync::Arc;

use rust_extensions::MyTimerTick;

use crate::app::AppContext;

pub struct MetricsUpdater {
    app: Arc<AppContext>,
}

impl MetricsUpdater {
    pub fn new(app: Arc<AppContext>) -> Self {
        Self { app }
    }
}

#[async_trait::async_trait]
impl MyTimerTick for MetricsUpdater {
    async fn tick(&self) {
        let topics = self.app.topics_snapshot.get().await;

        for topic in topics.snapshot.data {
            let topic_data = self.app.topics_list.get(topic.topic_id.as_str()).await;

            if topic_data.is_none() {
                continue;
            }

            let topic_data = topic_data.unwrap();

            let queue_size = topic_data.get_messages_amount_to_save().await;

            self.app
                .metrics_keeper
                .update_topic_queue_size(topic.topic_id.as_str(), queue_size)
                .await;
        }
    }
}
