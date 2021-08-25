use std::collections::HashMap;

use prometheus::{Encoder, Gauge, Opts, Registry, TextEncoder};
use tokio::sync::RwLock;

pub struct PrometheusMetrics {
    registry: Registry,
    gauges: RwLock<HashMap<String, Gauge>>,
}

impl PrometheusMetrics {
    pub fn new() -> Self {
        return Self {
            registry: Registry::new(),
            gauges: RwLock::new(HashMap::new()),
        };
    }
    async fn update_gauge_if_exists_amount(&self, name: &str, value: usize) -> bool {
        let read_access = self.gauges.read().await;

        if read_access.contains_key(name) {
            let gauge = read_access.get(name).unwrap();
            gauge.set(value as f64);
            return true;
        }

        return false;
    }

    async fn create_gauge(&self, name: &str, value: usize, gauge_opts: Opts) {
        let mut write_access = self.gauges.write().await;

        if !write_access.contains_key(name) {
            let gauge = Gauge::with_opts(gauge_opts).unwrap();
            self.registry.register(Box::new(gauge.clone())).unwrap();
            write_access.insert(name.to_string(), gauge);
        }

        let gauge = write_access.get(name).unwrap();
        gauge.set(value as f64);
    }

    pub async fn update_topic_queue_size(&self, name: &str, value: usize) {
        let name = name.replace('-', "_");
        if self
            .update_gauge_if_exists_amount(name.as_str(), value)
            .await
        {
            return;
        }

        let gauge_opts = Opts::new(
            format!("topic_{}_persist_queue_size", name),
            format!("{} persist queue size", name),
        );
        self.create_gauge(name.as_str(), value, gauge_opts).await;
    }

    pub fn build_prometheus_content(&self) -> String {
        let mut buffer = vec![];
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        encoder.encode(&metric_families, &mut buffer).unwrap();

        return String::from_utf8(buffer).unwrap();
    }
}
