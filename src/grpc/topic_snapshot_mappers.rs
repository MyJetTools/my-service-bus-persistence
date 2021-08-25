use crate::persistence_grpc::*;
use crate::toipics_snapshot::{
    QueueRangeProtobufModel, QueueSnapshotProtobufModel, TopicsDataProtobufModel,
    TopicsSnaphotProtobufModel,
};

// From Domain to Grpc-Contract
pub fn to_topic_snapshot_grpc_model(
    src: &TopicsSnaphotProtobufModel,
) -> TopicAndQueuesSnapshotGrpcModel {
    TopicAndQueuesSnapshotGrpcModel {
        topic_id: src.topic_id.to_string(),
        message_id: src.message_id,
        queue_snapshots: to_queue_snapshot_grpc_models_vec(src.queues.as_slice()),
    }
}

pub fn to_index_range_grpc_model(src: &QueueRangeProtobufModel) -> QueueIndexRangeGrpcModel {
    QueueIndexRangeGrpcModel {
        from_id: src.from_id,
        to_id: src.to_id,
    }
}

pub fn to_index_range_grpc_models_vec(
    src: &[QueueRangeProtobufModel],
) -> Vec<QueueIndexRangeGrpcModel> {
    src.iter()
        .map(|itm| to_index_range_grpc_model(itm))
        .collect()
}

pub fn to_queue_snapshot_grpc_model(src: &QueueSnapshotProtobufModel) -> QueueSnapshotGrpcModel {
    QueueSnapshotGrpcModel {
        queue_id: src.queue_id.to_string(),
        queue_type: 0, //TODO - посмотреть как эт используется в C#
        ranges: to_index_range_grpc_models_vec(&src.ranges),
    }
}

pub fn to_queue_snapshot_grpc_models_vec(
    src: &[QueueSnapshotProtobufModel],
) -> Vec<QueueSnapshotGrpcModel> {
    src.iter()
        .map(|itm| to_queue_snapshot_grpc_model(itm))
        .collect()
}

// From  Grpc-Contract To Domain

pub fn to_topics_data_protobuf_model(
    src: &SaveQueueSnapshotGrpcRequest,
) -> TopicsDataProtobufModel {
    TopicsDataProtobufModel {
        data: to_topic_snapshot_protobuf_models_vec(src.queue_snapshot.as_slice()),
    }
}

pub fn to_topic_snapshot_protobuf_models_vec(
    src: &[TopicAndQueuesSnapshotGrpcModel],
) -> Vec<TopicsSnaphotProtobufModel> {
    src.iter()
        .map(|itm| TopicsSnaphotProtobufModel {
            topic_id: itm.topic_id.to_string(),
            message_id: itm.message_id,
            not_used: 0,
            queues: to_queue_snapshot_protobuf_models_vec(itm.queue_snapshots.as_slice()),
        })
        .collect()
}

pub fn to_queue_range_protobuf_model(src: &QueueIndexRangeGrpcModel) -> QueueRangeProtobufModel {
    QueueRangeProtobufModel {
        from_id: src.from_id,
        to_id: src.to_id,
    }
}

pub fn to_queue_range_protobuf_models_vec(
    src: &[QueueIndexRangeGrpcModel],
) -> Vec<QueueRangeProtobufModel> {
    src.iter()
        .map(|itm| to_queue_range_protobuf_model(itm))
        .collect()
}

pub fn to_queue_snapshot_protobuf_model(
    src: &QueueSnapshotGrpcModel,
) -> QueueSnapshotProtobufModel {
    QueueSnapshotProtobufModel {
        queue_id: src.queue_id.to_string(),
        ranges: to_queue_range_protobuf_models_vec(&src.ranges),
    }
}

pub fn to_queue_snapshot_protobuf_models_vec(
    src: &[QueueSnapshotGrpcModel],
) -> Vec<QueueSnapshotProtobufModel> {
    src.iter()
        .map(|itm| to_queue_snapshot_protobuf_model(itm))
        .collect()
}
