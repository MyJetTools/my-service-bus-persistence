use my_service_bus_shared::protobuf_models::{
    QueueRangeProtobufModel, QueueSnapshotProtobufModel, TopicSnapshotProtobufModel,
};

use crate::persistence_grpc::*;

// From Domain to Grpc-Contract
pub fn to_topic_snapshot(src: &TopicSnapshotProtobufModel) -> TopicAndQueuesSnapshotGrpcModel {
    TopicAndQueuesSnapshotGrpcModel {
        topic_id: src.topic_id.to_string(),
        message_id: src.message_id,
        queue_snapshots: to_queue_snapshot_vec(src.queues.as_slice()),
    }
}

pub fn to_index_range(src: &QueueRangeProtobufModel) -> QueueIndexRangeGrpcModel {
    QueueIndexRangeGrpcModel {
        from_id: src.from_id,
        to_id: src.to_id,
    }
}

pub fn to_index_range_vec(src: &[QueueRangeProtobufModel]) -> Vec<QueueIndexRangeGrpcModel> {
    src.iter().map(|itm| to_index_range(itm)).collect()
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
