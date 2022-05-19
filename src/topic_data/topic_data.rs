use std::sync::atomic::{AtomicI64, Ordering};
use std::{collections::HashMap, sync::Arc};

use my_service_bus_shared::MessageId;
use tokio::sync::Mutex;

use crate::compressed_page::*;

use crate::{index_by_minute::YearlyIndexByMinute, uncompressed_page::*};

use super::topic_data_metrics::TopicDataMetrics;

pub struct TopicData {
    pub topic_id: String,
    pub uncompressed_pages_list: UncompressedPagesList,
    pub metrics: TopicDataMetrics,
    pub yearly_index_by_minute: Mutex<HashMap<u32, YearlyIndexByMinute>>,
    pub compressed_clusters: Mutex<HashMap<usize, Arc<CompressedCluster>>>,
    topic_message_id: AtomicI64,
}

impl TopicData {
    pub fn new(topic_id: &str, message_id: MessageId) -> Self {
        Self {
            topic_id: topic_id.to_string(),
            uncompressed_pages_list: UncompressedPagesList::new(),
            metrics: TopicDataMetrics::new(),
            yearly_index_by_minute: Mutex::new(HashMap::new()),
            compressed_clusters: Mutex::new(HashMap::new()),
            topic_message_id: AtomicI64::new(message_id),
        }
    }

    pub async fn get_messages_amount_to_save(&self) -> usize {
        let mut result = 0;
        for page in &self.uncompressed_pages_list.get_all().await {
            result += page.metrics.get_messages_amount_to_save();
        }
        result
    }

    pub fn update_max_message_id(&self, message_id: MessageId) {
        //TODO -  Проверить - правильно ли мы понимаем максимум
        self.topic_message_id
            .fetch_max(message_id, Ordering::SeqCst);
    }

    pub fn get_max_message_id(&self) -> MessageId {
        self.topic_message_id.load(Ordering::SeqCst)
    }
}
