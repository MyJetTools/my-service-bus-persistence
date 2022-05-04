use std::{collections::HashMap, sync::Arc};

use tokio::sync::RwLock;

use super::TopicData;

pub struct TopicsDataList {
    data: RwLock<HashMap<String, Arc<TopicData>>>,
}

impl TopicsDataList {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
        }
    }

    pub async fn get(&self, topic_id: &str) -> Option<Arc<TopicData>> {
        let read_access = self.data.read().await;
        let result = read_access.get(topic_id)?;
        return Some(result.clone());
    }

    pub async fn get_all(&self) -> Vec<Arc<TopicData>> {
        let read_access = self.data.read().await;
        read_access.values().map(|v| v.clone()).collect()
    }

    pub async fn create_topic_data(&self, topic_id: &str) -> bool {
        let mut write_access = self.data.write().await;

        if write_access.contains_key(topic_id) {
            return false;
        }

        let topic_data = Arc::new(TopicData::new(topic_id));
        write_access.insert(topic_id.to_string(), topic_data);
        return true;
    }

    pub async fn init_topic_data(&self, topic_id: &str) -> Arc<TopicData> {
        let mut write_access = self.data.write().await;
        let topic_data = Arc::new(TopicData::new(topic_id));
        write_access.insert(topic_id.to_string(), topic_data.clone());
        topic_data
    }
}
