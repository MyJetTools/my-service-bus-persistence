use prometheus::{IntGaugeVec, Registry};
const TOPIC_LABEL: &str = "topic";

pub struct GaugeByTopic(IntGaugeVec);

impl GaugeByTopic {
    pub fn new(registry: &Registry, name: &str, help: &str) -> Self {
        let gauge = IntGaugeVec::new(prometheus::Opts::new(name, help), &[TOPIC_LABEL]).unwrap();

        registry.register(Box::new(gauge.clone())).unwrap();
        Self(gauge)
    }

    pub fn update_value(&self, topic_id: &str, value: i64) {
        self.0.with_label_values(&[topic_id]).set(value);
    }

    pub fn remove_topic(&self, topic_id: &str) {
        let result = self.0.remove_label_values(&[topic_id]);

        if let Err(err) = result {
            println!(
                "Failed to remove topic {} from prometheus metrics: {}",
                topic_id, err
            );
        }
    }
}
