use super::protobuf_model::TopicsDataProtobufModel;

#[derive(Clone)]
pub struct CurrentTopicsSnapshot {
    pub snapshot_id: i64,
    pub last_saved_snapshot_id: i64,
    pub snapshot: TopicsDataProtobufModel,
}

impl CurrentTopicsSnapshot {
    pub fn new(snapshot: TopicsDataProtobufModel) -> Self {
        Self {
            snapshot,
            snapshot_id: 0,
            last_saved_snapshot_id: 0,
        }
    }

    pub fn update(&mut self, snapshot: TopicsDataProtobufModel) {
        self.snapshot = snapshot;
        self.snapshot_id += 1;
    }

    pub fn saved(&mut self, saved_id: i64) {
        self.last_saved_snapshot_id = saved_id;
    }
}
