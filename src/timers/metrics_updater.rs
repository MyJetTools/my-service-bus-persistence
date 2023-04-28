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
        let topics_list = self.app.topics_snapshot.get_topics_list().await;

        let mut metrics = HashMap::new();

        for topic_id in &topics_list {
            match self.app.topics_list.get(topic_id).await {
                Some(topic_data) => {
                    let queue_size = topic_data.pages_list.get_messages_amount_to_save().await;
                    metrics.insert(
                        topic_id.as_str(),
                        PrometheusMetricsToUpdate {
                            not_persisted_size: queue_size.amount,
                            content_size: queue_size.size,
                        },
                    );
                }
                None => {
                    metrics.insert(
                        topic_id.as_str(),
                        PrometheusMetricsToUpdate {
                            not_persisted_size: 0,
                            content_size: 0,
                        },
                    );
                }
            }
        }

        self.app.metrics_keeper.update(metrics).await;
    }
}
