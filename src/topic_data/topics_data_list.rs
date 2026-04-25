use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use rust_extensions::sorted_vec::SortedVecOfArcWithStrKey;

use super::TopicData;

struct TopicsDataInner {
    data: SortedVecOfArcWithStrKey<TopicData>,
    as_vec: Arc<Vec<Arc<TopicData>>>,
}

impl TopicsDataInner {
    fn empty() -> Self {
        Self {
            data: SortedVecOfArcWithStrKey::new(),
            as_vec: Arc::new(Vec::new()),
        }
    }

    fn from_data(data: SortedVecOfArcWithStrKey<TopicData>) -> Self {
        let as_vec = Arc::new(data.as_slice().to_vec());
        Self { data, as_vec }
    }
}

pub struct TopicsDataList {
    inner: ArcSwap<TopicsDataInner>,
    write_lock: Mutex<()>,
}

impl TopicsDataList {
    pub fn new() -> Self {
        Self {
            inner: ArcSwap::from_pointee(TopicsDataInner::empty()),
            write_lock: Mutex::new(()),
        }
    }

    pub fn get(&self, topic_id: &str) -> Option<Arc<TopicData>> {
        self.inner.load().data.get(topic_id).cloned()
    }

    pub fn get_all(&self) -> Arc<Vec<Arc<TopicData>>> {
        self.inner.load().as_vec.clone()
    }

    pub fn create_topic_data(&self, topic_id: &str) -> bool {
        let _guard = self.write_lock.lock().unwrap();
        let current = self.inner.load_full();

        if current.data.get(topic_id).is_some() {
            return false;
        }

        let mut new_data = current.data.clone();
        new_data.insert_or_replace(Arc::new(TopicData::new(topic_id)));

        self.inner
            .store(Arc::new(TopicsDataInner::from_data(new_data)));

        true
    }

    pub fn init_topic_data(&self, topic_id: &str) -> Arc<TopicData> {
        let _guard = self.write_lock.lock().unwrap();
        let current = self.inner.load_full();

        if let Some(existing) = current.data.get(topic_id) {
            return existing.clone();
        }

        let topic_data = Arc::new(TopicData::new(topic_id));

        let mut new_data = current.data.clone();
        new_data.insert_or_replace(topic_data.clone());

        self.inner
            .store(Arc::new(TopicsDataInner::from_data(new_data)));

        topic_data
    }

    pub fn remove(&self, topic_id: &str) {
        let _guard = self.write_lock.lock().unwrap();
        let current = self.inner.load_full();

        let mut new_data = current.data.clone();
        if new_data.remove(topic_id).is_none() {
            return;
        }

        self.inner
            .store(Arc::new(TopicsDataInner::from_data(new_data)));
    }

    // TODO: soft-delete (`delete`) is being reworked. See `TODO.md`.
    // pub fn delete(&self, topic_id: &str) -> Option<Arc<TopicData>> {
    //     let _guard = self.write_lock.lock().unwrap();
    //     let current = self.inner.load_full();
    //
    //     let mut new_data = current.data.clone();
    //     let removed = new_data.remove(topic_id)?;
    //
    //     let mut new_deleted = current.deleted.clone();
    //     new_deleted.insert(topic_id.to_string());
    //
    //     self.inner
    //         .store(Arc::new(TopicsDataInner::from_parts(new_data, new_deleted)));
    //
    //     Some(removed)
    // }
}
