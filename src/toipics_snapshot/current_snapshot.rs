use my_service_bus_abstractions::MessageId;
use my_service_bus_shared::protobuf_models::TopicsSnapshotProtobufModel;
use tokio::sync::{Mutex, RwLock};

use super::blob_repository::TopicsSnapshotBlobRepository;

#[derive(Clone)]
pub struct TopicsSnapshotData {
    pub snapshot_id: i64,
    pub last_saved_snapshot_id: i64,
    pub snapshot: TopicsSnapshotProtobufModel,
}

impl TopicsSnapshotData {
    pub fn new(snapshot: TopicsSnapshotProtobufModel) -> Self {
        Self {
            snapshot,
            snapshot_id: 0,
            last_saved_snapshot_id: 0,
        }
    }

    pub fn update(&mut self, snapshot: TopicsSnapshotProtobufModel) {
        self.snapshot = snapshot;
        self.snapshot_id += 1;
    }

    pub fn update_snapshot_id(&mut self, saved_id: i64) {
        self.last_saved_snapshot_id = saved_id;
    }
}

pub struct CurrentTopicsSnapshot {
    data: RwLock<TopicsSnapshotData>,
    pub blob: Mutex<TopicsSnapshotBlobRepository>,
}

impl CurrentTopicsSnapshot {
    pub async fn new(mut blob: TopicsSnapshotBlobRepository) -> Self {
        let snapshot = blob.read().await.unwrap();
        Self {
            data: RwLock::new(TopicsSnapshotData::new(snapshot)),
            blob: Mutex::new(blob),
        }
    }

    pub async fn get(&self) -> TopicsSnapshotData {
        let read_access = self.data.read().await;
        read_access.clone()
    }

    pub async fn update(&self, snapshot: TopicsSnapshotProtobufModel) {
        let mut write_access = self.data.write().await;
        write_access.update(snapshot);
    }

    pub async fn update_snapshot_id_as_saved(&self, saved_id: i64) {
        let mut write_access = self.data.write().await;
        write_access.update_snapshot_id(saved_id);
    }

    pub async fn get_snapshot_if_there_are_chages(&self) -> Option<TopicsSnapshotData> {
        let read_access = self.data.read().await;
        if read_access.snapshot_id == read_access.last_saved_snapshot_id {
            return None;
        }

        return Some(read_access.clone());
    }

    pub async fn get_current_message_id(&self, topic_id: &str) -> Option<MessageId> {
        let read_access = self.data.read().await;

        for topic in &read_access.snapshot.data {
            if topic.topic_id == topic_id {
                return Some(topic.get_message_id());
            }
        }

        None
    }
}
