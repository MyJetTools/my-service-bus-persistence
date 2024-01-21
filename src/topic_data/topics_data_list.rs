use std::{collections::HashMap, sync::Arc};

use rust_extensions::sorted_vec::SortedVecOfArcWithStrKey;
use tokio::sync::RwLock;

use super::TopicData;

pub struct TopicDataInner {
    data: SortedVecOfArcWithStrKey<TopicData>,
    deleted: HashMap<String, ()>,
}

impl TopicDataInner {
    pub fn new() -> Self {
        Self {
            data: SortedVecOfArcWithStrKey::new(),
            deleted: HashMap::new(),
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
        read_access.data.to_vec_cloned()
    }

    pub async fn create_topic_data(&self, topic_id: &str) -> bool {
        let mut write_access = self.data.write().await;

        if write_access.deleted.contains_key(topic_id) {
            panic!("Topic {} is deleted", topic_id);
        }

        match write_access.data.insert_or_if_not_exists(topic_id) {
            rust_extensions::sorted_vec::InsertIfNotExists::Insert(entry) => {
                let topic_data = Arc::new(TopicData::new(topic_id));
                entry.insert(topic_data);
                true
            }
            rust_extensions::sorted_vec::InsertIfNotExists::Exists(_) => false,
        }
    }

    pub async fn init_topic_data(&self, topic_id: &str) -> Arc<TopicData> {
        let mut write_access = self.data.write().await;
        match write_access.data.insert_or_if_not_exists(topic_id) {
            rust_extensions::sorted_vec::InsertIfNotExists::Insert(entry) => {
                let topic_data = Arc::new(TopicData::new(topic_id));
                entry.insert(topic_data.clone());
                topic_data
            }
            rust_extensions::sorted_vec::InsertIfNotExists::Exists(index) => {
                write_access.data.get_by_index(index).unwrap().clone()
            }
        }
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
