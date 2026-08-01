use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use rust_extensions::SortedVecOfArcWith2StrKey;

use crate::topic_key::TopicKeyRef;

use super::TopicData;

struct TopicsDataInner {
    data: SortedVecOfArcWith2StrKey<TopicData>,
    as_vec: Arc<Vec<Arc<TopicData>>>,
}

impl TopicsDataInner {
    fn empty() -> Self {
        Self {
            data: SortedVecOfArcWith2StrKey::new(),
            as_vec: Arc::new(Vec::new()),
        }
    }

    fn from_data(data: SortedVecOfArcWith2StrKey<TopicData>) -> Self {
        let as_vec = Arc::new(data.iter().cloned().collect());
        Self { data, as_vec }
    }
}

/// `SortedVecOfArcWith2StrKey` derives `Clone` with a `TValue: Clone` bound it does not actually
/// need (only the `Arc`s are cloned), and `TopicData` is not `Clone`. Copy-on-write writers are
/// rare - topic creation / removal - so we rebuild the container from the existing `Arc`s, which
/// are already in key order.
fn copy_of(src: &SortedVecOfArcWith2StrKey<TopicData>) -> SortedVecOfArcWith2StrKey<TopicData> {
    let mut result = SortedVecOfArcWith2StrKey::new();

    for topic_data in src.iter() {
        result.insert_or_replace(topic_data.clone());
    }

    result
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

    pub fn get(&self, topic_key: TopicKeyRef<'_>) -> Option<Arc<TopicData>> {
        self.inner
            .load()
            .data
            .get(topic_key.namespace, topic_key.topic_id)
            .cloned()
    }

    pub fn get_all(&self) -> Arc<Vec<Arc<TopicData>>> {
        self.inner.load().as_vec.clone()
    }

    pub fn create_topic_data(&self, topic_key: TopicKeyRef<'_>) -> bool {
        let _guard = self.write_lock.lock().unwrap();
        let current = self.inner.load_full();

        if current
            .data
            .get(topic_key.namespace, topic_key.topic_id)
            .is_some()
        {
            return false;
        }

        let mut new_data = copy_of(&current.data);
        new_data.insert_or_replace(Arc::new(TopicData::new(topic_key)));

        self.inner
            .store(Arc::new(TopicsDataInner::from_data(new_data)));

        true
    }

    pub fn init_topic_data(&self, topic_key: TopicKeyRef<'_>) -> Arc<TopicData> {
        let _guard = self.write_lock.lock().unwrap();
        let current = self.inner.load_full();

        if let Some(existing) = current.data.get(topic_key.namespace, topic_key.topic_id) {
            return existing.clone();
        }

        let topic_data = Arc::new(TopicData::new(topic_key));

        let mut new_data = copy_of(&current.data);
        new_data.insert_or_replace(topic_data.clone());

        self.inner
            .store(Arc::new(TopicsDataInner::from_data(new_data)));

        topic_data
    }

    pub fn remove(&self, topic_key: TopicKeyRef<'_>) {
        let _guard = self.write_lock.lock().unwrap();
        let current = self.inner.load_full();

        let mut new_data = copy_of(&current.data);
        if new_data
            .remove(topic_key.namespace, topic_key.topic_id)
            .is_none()
        {
            return;
        }

        self.inner
            .store(Arc::new(TopicsDataInner::from_data(new_data)));
    }

    // TODO: soft-delete (`delete`) is being reworked. See `TODO.md`.
    // pub fn delete(&self, topic_key: TopicKeyRef<'_>) -> Option<Arc<TopicData>> {
    //     let _guard = self.write_lock.lock().unwrap();
    //     let current = self.inner.load_full();
    //
    //     let mut new_data = copy_of(&current.data);
    //     let removed = new_data.remove(topic_key.namespace, topic_key.topic_id)?;
    //
    //     let mut new_deleted = current.deleted.clone();
    //     new_deleted.insert(topic_key.to_owned_key());
    //
    //     self.inner
    //         .store(Arc::new(TopicsDataInner::from_parts(new_data, new_deleted)));
    //
    //     Some(removed)
    // }
}
