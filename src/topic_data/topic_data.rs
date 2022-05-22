use std::{collections::HashMap, sync::atomic::AtomicI64};

use my_service_bus_shared::MessageId;
use tokio::sync::Mutex;

use crate::{index_by_minute::YearlyIndexByMinute, uncompressed_page::UncompressedPagesList};

use super::topic_data_metrics::TopicDataMetrics;

pub struct TopicData {
    pub topic_id: String,
    pub uncompressed_pages_list: UncompressedPagesList,
    pub metrics: TopicDataMetrics,
    pub yearly_index_by_minute: Mutex<HashMap<u32, YearlyIndexByMinute>>,
    current_message_id: AtomicI64,
}

impl TopicData {
    pub fn new(topic_id: &str, max_message_id: MessageId) -> Self {
        Self {
            topic_id: topic_id.to_string(),
            uncompressed_pages_list: UncompressedPagesList::new(),
            metrics: TopicDataMetrics::new(),
            yearly_index_by_minute: Mutex::new(HashMap::new()),
            current_message_id: AtomicI64::new(max_message_id),
        }
    }

    pub async fn get_messages_amount_to_save(&self) -> usize {
        let mut result = 0;
        for page in &self.uncompressed_pages_list.get_all().await {
            result += page.metrics.get_messages_amount_to_save();
        }
        result
    }

    pub fn update_current_message_id(&self, max_message_id: MessageId) {
        self.current_message_id
            .store(max_message_id, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn get_current_message_id(&self) -> MessageId {
        self.current_message_id
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}
