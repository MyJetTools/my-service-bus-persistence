use my_service_bus_shared::protobuf_models::TopicsSnapshotProtobufModel;
use tokio::sync::RwLock;

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
}

impl CurrentTopicsSnapshot {
    pub fn new(snapshot: TopicsSnapshotProtobufModel) -> Self {
        Self {
            data: RwLock::new(TopicsSnapshotData::new(snapshot)),
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
}
