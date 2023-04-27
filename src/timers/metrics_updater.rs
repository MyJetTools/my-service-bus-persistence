use std::{collections::HashMap, sync::Arc};

use rust_extensions::MyTimerTick;

use crate::app::{AppContext, PrometheusMetricsToUpdate};

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
        let topics = self.app.topics_list.get_all().await;

        let mut metrics = HashMap::new();

        for topic_data in &topics {
            let queue_size = topic_data.pages_list.get_messages_amount_to_save().await;
            metrics.insert(
                topic_data.topic_id.as_str(),
                PrometheusMetricsToUpdate {
                    not_persisted_size: queue_size.amount,
                    content_size: queue_size.size,
                },
            );
        }

        self.app.metrics_keeper.update(metrics).await;
    }
}
