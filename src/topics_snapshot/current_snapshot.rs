use my_service_bus_abstractions::MessageId;
use my_service_bus_shared::protobuf_models::TopicsSnapshotProtobufModel;
use tokio::sync::RwLock;

use crate::app::Logs;

use super::page_blob_storage::TopicsSnapshotPageBlobStorage;

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
    pub blob: TopicsSnapshotPageBlobStorage,
}

impl CurrentTopicsSnapshot {
    pub async fn read_or_create(blob: TopicsSnapshotPageBlobStorage) -> Self {
        let snapshot = blob.read_or_create_topics_snapshot().await.unwrap();
        Self {
            data: RwLock::new(TopicsSnapshotData::new(snapshot)),
            blob,
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

    pub async fn get_snapshot_if_there_are_changes(&self) -> Option<TopicsSnapshotData> {
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

    pub async fn flush_topics_snapshot_to_blob(&self, logs: &Logs) {
        let snapshot = self.get_snapshot_if_there_are_changes().await;

        if snapshot.is_none() {
            return;
        }

        let snapshot = snapshot.unwrap();

        let mut attempt_no = 0;

        loop {
            let result = { self.blob.write_topics_snapshot(&snapshot.snapshot).await };

            if let Err(err) = result {
                logs.write(
                    crate::app::LogLevel::Error,
                    "Write Topics Snapshot".to_string(),
                    format!(
                        "Can not snapshot with ID #{}. Attempt:{}. Err: {:?}",
                        snapshot.snapshot_id, attempt_no, err
                    ),
                    None,
                );

                if attempt_no >= 5 {
                    return;
                }

                attempt_no += 1;

                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            } else {
                self.update_snapshot_id_as_saved(snapshot.snapshot_id).await;
                return;
            }
        }
    }
}
