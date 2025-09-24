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

/*
impl Into<TopicAndQueuesSnapshotGrpcModel> for &TopicSnapshotProtobufModel {
    fn into(self) -> TopicAndQueuesSnapshotGrpcModel {
        let queue_snapshots = self
            .queues
            .into_iter()
            .map(|itm| QueueSnapshotGrpcModel {
                queue_id: itm.queue_id,
                ranges: itm
                    .ranges
                    .into_iter()
                    .map(|itm| QueueIndexRangeGrpcModel {
                        from_id: itm.get_from_id().get_value(),
                        to_id: itm.get_to_id().get_value(),
                    })
                    .collect(),
                queue_type: itm.queue_type,
            })
            .collect();
        TopicAndQueuesSnapshotGrpcModel {
            message_id: self.get_message_id().get_value(),
            queue_snapshots,
            persist: self.persist,
            topic_id: self.topic_id.to_string(),
        }
    }
}
 */
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
