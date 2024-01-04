use std::{collections::BTreeMap, sync::Arc};

use tokio::sync::RwLock;

use super::TopicData;

pub struct TopicDataInner {
    data: BTreeMap<String, Arc<TopicData>>,
    deleted: BTreeMap<String, ()>,
}

impl TopicDataInner {
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
            deleted: BTreeMap::new(),
        }
    }
}

pub struct TopicsDataList {
    data: RwLock<TopicDataInner>,
}

impl TopicsDataList {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(TopicDataInner::new()),
        }
    }

    pub async fn get(&self, topic_id: &str) -> Option<Arc<TopicData>> {
        let read_access = self.data.read().await;

        if read_access.deleted.contains_key(topic_id) {
            panic!("Topic {} is deleted", topic_id)
        }

        let result = read_access.data.get(topic_id)?;
        return Some(result.clone());
    }

    pub async fn get_all(&self) -> Vec<Arc<TopicData>> {
        let read_access = self.data.read().await;

        read_access.data.values().map(|v| v.clone()).collect()
    }

    pub async fn create_topic_data(&self, topic_id: &str) -> bool {
        let mut write_access = self.data.write().await;

        if write_access.deleted.contains_key(topic_id) {
            panic!("Topic {} is deleted", topic_id);
        }

        if write_access.data.contains_key(topic_id) {
            return false;
        }

        let topic_data = Arc::new(TopicData::new(topic_id));
        write_access.data.insert(topic_id.to_string(), topic_data);
        return true;
    }

    pub async fn init_topic_data(&self, topic_id: &str) -> Arc<TopicData> {
        let mut write_access = self.data.write().await;
        let topic_data = Arc::new(TopicData::new(topic_id));
        write_access
            .data
            .insert(topic_id.to_string(), topic_data.clone());
        topic_data
    }

    pub async fn remove(&self, topic_id: &str) {
        let mut write_access = self.data.write().await;
        write_access.data.remove(topic_id);
    }

    pub async fn delete(&self, topic_id: &str) -> Option<Arc<TopicData>> {
        let mut write_access = self.data.write().await;
        let result = write_access.data.remove(topic_id);

        if result.is_some() {
            write_access.deleted.insert(topic_id.to_string(), ());
        }

        result
    }
}
