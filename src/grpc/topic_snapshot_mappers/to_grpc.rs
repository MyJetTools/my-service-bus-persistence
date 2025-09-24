use crate::{
    persistence_grpc::*,
    topics_snapshot::{
        QueueRangeProtobufModel, QueueSnapshotProtobufModel, TopicSnapshotProtobufModel,
    },
};

impl From<TopicSnapshotProtobufModel> for TopicAndQueuesSnapshotGrpcModel {
    fn from(value: TopicSnapshotProtobufModel) -> Self {
        let message_id = value.get_message_id().get_value();
        Self {
            topic_id: value.topic_id,
            message_id,
            queue_snapshots: value.queues.into_iter().map(|itm| itm.into()).collect(),
            persist: value.persist,
        }
    }
}

impl Into<QueueSnapshotGrpcModel> for QueueSnapshotProtobufModel {
    fn into(self) -> QueueSnapshotGrpcModel {
        let ranges = self
            .ranges
            .into_iter()
            .map(|itm| QueueIndexRangeGrpcModel {
                from_id: itm.get_from_id().get_value(),
                to_id: itm.get_to_id().get_value(),
            })
            .collect();
        QueueSnapshotGrpcModel {
            queue_id: self.queue_id,
            ranges,
            queue_type: self.queue_type,
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

impl From<Vec<u8>> for CompressedMessageChunkModel {
    fn from(value: Vec<u8>) -> Self {
        CompressedMessageChunkModel { chunk: value }
    }
}
