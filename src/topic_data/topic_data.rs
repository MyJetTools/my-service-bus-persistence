use std::collections::HashMap;

use tokio::sync::Mutex;

use crate::{index_by_minute::YearlyIndexByMinute, message_pages::PagesList};

use super::topic_data_metrics::TopicDataMetrics;

pub struct TopicData {
    pub topic_id: String,
    pub pages_list: PagesList,
    pub metrics: TopicDataMetrics,
    //TODO - не забыть GC
    pub yearly_index_by_minute: Mutex<HashMap<u32, YearlyIndexByMinute>>,
}

impl TopicData {
    pub fn new(topic_id: &str) -> Self {
        Self {
            topic_id: topic_id.to_string(),
            pages_list: PagesList::new(),
            metrics: TopicDataMetrics::new(),
            yearly_index_by_minute: Mutex::new(HashMap::new()),
        }
    }

    pub fn get_messages_amount_to_save(&self) -> usize {
        todo!("Implement");
    }
}
