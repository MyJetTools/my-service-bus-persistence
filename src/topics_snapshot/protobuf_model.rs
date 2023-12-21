use my_service_bus::abstractions::MessageId;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TopicsSnapshotProtobufModel {
    #[prost(message, repeated, tag = "1")]
    pub data: Vec<TopicSnapshotProtobufModel>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TopicsSnapshotProtobufModelV2 {
    #[prost(message, repeated, tag = "1")]
    pub data: Vec<TopicSnapshotProtobufModel>,
    #[prost(message, repeated, tag = "2")]
    pub deleted_topics: Vec<DeletedTopicProtobufModel>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TopicSnapshotProtobufModel {
    #[prost(string, tag = "1")]
    pub topic_id: String,

    #[prost(int64, tag = "2")]
    message_id: i64,

    #[prost(int32, tag = "3")]
    pub not_used: i32,

    #[prost(message, repeated, tag = "4")]
    pub queues: Vec<QueueSnapshotProtobufModel>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeletedTopicProtobufModel {
    #[prost(string, tag = "1")]
    pub topic_id: String,
    #[prost(int64, tag = "2")]
    pub message_id: i64,
    #[prost(int64, tag = "3")]
    pub gc_after: i64,
}

impl TopicSnapshotProtobufModel {
    pub fn new(
        topic_id: String,
        message_id: MessageId,
        queues: Vec<QueueSnapshotProtobufModel>,
    ) -> Self {
        Self {
            topic_id,
            message_id: message_id.get_value(),
            not_used: 0,
            queues,
        }
    }
    pub fn get_message_id(&self) -> MessageId {
        self.message_id.into()
    }
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueueSnapshotProtobufModel {
    #[prost(string, tag = "1")]
    pub queue_id: ::prost::alloc::string::String,

    #[prost(message, repeated, tag = "2")]
    pub ranges: Vec<QueueRangeProtobufModel>,

    #[prost(int32, tag = "3")]
    pub queue_type: i32,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QueueRangeProtobufModel {
    #[prost(int64, tag = "1")]
    from_id: i64,

    #[prost(int64, tag = "2")]
    to_id: i64,
}

impl QueueRangeProtobufModel {
    pub fn new(from_id: MessageId, to_id: MessageId) -> Self {
        Self {
            from_id: from_id.into(),
            to_id: to_id.into(),
        }
    }

    pub fn get_from_id(&self) -> MessageId {
        self.from_id.into()
    }

    pub fn get_to_id(&self) -> MessageId {
        self.to_id.into()
    }
}
