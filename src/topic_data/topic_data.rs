use std::{collections::HashMap, sync::Arc};

use tokio::sync::Mutex;

use crate::{
    index_by_minute::YearlyIndexByMinute,
    message_pages::{CompressedCluster, PagesList},
};

use super::topic_data_metrics::TopicDataMetrics;

pub struct TopicData {
    pub topic_id: String,
    pub pages_list: PagesList,
    pub metrics: TopicDataMetrics,
    pub yearly_index_by_minute: Mutex<HashMap<u32, YearlyIndexByMinute>>,
    pub compressed_clusters: Mutex<HashMap<usize, Arc<CompressedCluster>>>,
}

impl TopicData {
    pub fn new(topic_id: &str) -> Self {
        Self {
            topic_id: topic_id.to_string(),
            pages_list: PagesList::new(),
            metrics: TopicDataMetrics::new(),
            yearly_index_by_minute: Mutex::new(HashMap::new()),
            compressed_clusters: Mutex::new(HashMap::new()),
        }
    }

    pub async fn get_messages_amount_to_save(&self) -> usize {
        let mut result = 0;
        for page in &self.pages_list.get_all().await {
            result += page.get_messages_amount_to_save();
        }
        result
    }
}
