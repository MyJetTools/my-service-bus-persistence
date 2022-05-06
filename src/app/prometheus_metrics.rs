use prometheus::{Encoder, IntGaugeVec, Opts, Registry, TextEncoder};

pub struct PrometheusMetrics {
    registry: Registry,
    topic_persist_queue_size: IntGaugeVec,
}

const TOPIC_LABEL: &str = "topic";

impl PrometheusMetrics {
    pub fn new() -> Self {
        let topic_persist_queue_size = create_topic_persist_queue_size_gauge();
        return Self {
            registry: Registry::new(),
            topic_persist_queue_size,
        };
    }
    pub fn update_topic_persist_queue_size(&self, topic_id: &str, value: usize) {
        let value = value as i64;
        self.topic_persist_queue_size
            .with_label_values(&[topic_id])
            .set(value);
    }

    pub fn build_prometheus_content(&self) -> String {
        let mut buffer = vec![];
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        encoder.encode(&metric_families, &mut buffer).unwrap();

        return String::from_utf8(buffer).unwrap();
    }
}

fn create_topic_persist_queue_size_gauge() -> IntGaugeVec {
    let gauge_opts = Opts::new(
        format!("topic_persist_queue_size"),
        format!("topic persist queue size"),
    );

    let lables = &[TOPIC_LABEL];
    IntGaugeVec::new(gauge_opts, lables).unwrap()
}
