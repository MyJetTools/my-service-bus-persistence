use std::collections::{HashMap, HashSet};

use prometheus::{Encoder, Registry, TextEncoder};
use tokio::sync::Mutex;

use super::GaugeByTopic;

pub struct PrometheusMetricsToUpdate {
    pub not_persisted_size: usize,
    pub content_size: usize,
}

pub struct PrometheusMetrics {
    registry: Registry,
    topic_persist_queue_size: GaugeByTopic,
    cached_messages_size: GaugeByTopic,
    active_topics: Mutex<HashSet<String>>,
}

impl PrometheusMetrics {
    pub fn new() -> Self {
        let registry = Registry::new();
        let topic_persist_queue_size = GaugeByTopic::new(
            &registry,
            "topic_persist_queue_size",
            "Topic persist queue size",
        );

        let cached_messages_size =
            GaugeByTopic::new(&registry, "cached_messages_size", "Cached messages size");

        return Self {
            registry,
            topic_persist_queue_size,
            cached_messages_size,
            active_topics: Mutex::new(HashSet::new()),
        };
    }
    pub async fn update(&self, mut update_data: HashMap<&str, PrometheusMetricsToUpdate>) {
        let mut active_topics = self.active_topics.lock().await;

        for active_topic_id in active_topics.iter() {
            match update_data.remove(active_topic_id.as_str()) {
                Some(metrics) => {
                    self.topic_persist_queue_size
                        .update_value(active_topic_id.as_str(), metrics.not_persisted_size as i64);

                    self.cached_messages_size
                        .update_value(active_topic_id.as_str(), metrics.content_size as i64);
                }
                None => {
                    self.topic_persist_queue_size
                        .remove_topic(active_topic_id.as_str());

                    self.cached_messages_size
                        .remove_topic(active_topic_id.as_str());
                }
            }
        }

        for (topic_id, metrics) in update_data {
            self.topic_persist_queue_size
                .update_value(topic_id, metrics.not_persisted_size as i64);

            self.cached_messages_size
                .update_value(topic_id, metrics.content_size as i64);

            active_topics.insert(topic_id.to_string());
        }
    }

    pub fn build_prometheus_content(&self) -> String {
        let mut buffer = vec![];
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        encoder.encode(&metric_families, &mut buffer).unwrap();

        return String::from_utf8(buffer).unwrap();
    }
}
