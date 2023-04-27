use crate::{
    persistence_grpc::*,
    topics_snapshot::{
        QueueRangeProtobufModel, QueueSnapshotProtobufModel, TopicSnapshotProtobufModel,
    },
};

impl Into<TopicAndQueuesSnapshotGrpcModel> for &TopicSnapshotProtobufModel {
    fn into(self) -> TopicAndQueuesSnapshotGrpcModel {
        TopicAndQueuesSnapshotGrpcModel {
            topic_id: self.topic_id.to_string(),
            message_id: self.get_message_id().get_value(),
            queue_snapshots: to_queue_snapshot_vec(self.queues.as_slice()),
        }
    }
}

impl Into<QueueIndexRangeGrpcModel> for &QueueRangeProtobufModel {
    fn into(self) -> QueueIndexRangeGrpcModel {
        QueueIndexRangeGrpcModel {
            from_id: self.get_from_id().get_value(),
            to_id: self.get_to_id().get_value(),
        }
    }
}

pub fn to_index_range_vec(src: &[QueueRangeProtobufModel]) -> Vec<QueueIndexRangeGrpcModel> {
    src.iter().map(|itm| itm.into()).collect()
}

pub fn to_queue_snapshot(src: &QueueSnapshotProtobufModel) -> QueueSnapshotGrpcModel {
    QueueSnapshotGrpcModel {
        queue_id: src.queue_id.to_string(),
        queue_type: src.queue_type,
        ranges: to_index_range_vec(&src.ranges),
    }
}

pub fn to_queue_snapshot_vec(src: &[QueueSnapshotProtobufModel]) -> Vec<QueueSnapshotGrpcModel> {
    src.iter().map(|itm| to_queue_snapshot(itm)).collect()
}
