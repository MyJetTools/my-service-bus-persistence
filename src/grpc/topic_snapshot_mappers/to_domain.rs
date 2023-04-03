use my_service_bus_abstractions::AsMessageId;
use my_service_bus_shared::protobuf_models::{
    QueueRangeProtobufModel, QueueSnapshotProtobufModel, TopicSnapshotProtobufModel,
    TopicsSnapshotProtobufModel,
};

use crate::persistence_grpc::*;

pub fn to_topics_data(src: &SaveQueueSnapshotGrpcRequest) -> TopicsSnapshotProtobufModel {
    TopicsSnapshotProtobufModel {
        data: to_topic_snapshot_vec(src.queue_snapshot.as_slice()),
    }
}

pub fn to_topic_snapshot_vec(
    src: &[TopicAndQueuesSnapshotGrpcModel],
) -> Vec<TopicSnapshotProtobufModel> {
    src.iter()
        .map(|itm| {
            TopicSnapshotProtobufModel::new(
                itm.topic_id.to_string(),
                itm.message_id.as_message_id(),
                to_queue_snapshot_vec(itm.queue_snapshots.as_slice()),
            )
        })
        .collect()
}

pub fn to_queue_range(src: &QueueIndexRangeGrpcModel) -> QueueRangeProtobufModel {
    QueueRangeProtobufModel::new(src.from_id.as_message_id(), src.to_id.as_message_id())
}

pub fn to_queue_range_vec(src: &[QueueIndexRangeGrpcModel]) -> Vec<QueueRangeProtobufModel> {
    src.iter().map(|itm| to_queue_range(itm)).collect()
}

pub fn to_queue_snapshot(src: &QueueSnapshotGrpcModel) -> QueueSnapshotProtobufModel {
    QueueSnapshotProtobufModel {
        queue_id: src.queue_id.to_string(),
        ranges: to_queue_range_vec(&src.ranges),
        queue_type: src.queue_type() as i32,
    }
}

pub fn to_queue_snapshot_vec(src: &[QueueSnapshotGrpcModel]) -> Vec<QueueSnapshotProtobufModel> {
    src.iter().map(|itm| to_queue_snapshot(itm)).collect()
}
