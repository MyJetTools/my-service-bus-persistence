use crate::bcl_proto::BclDateTime;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MessageProtobufModel {
    #[prost(int64, tag = "1")]
    pub message_id: i64,
    #[prost(message, tag = "2")]
    pub created: Option<BclDateTime>,
    #[prost(bytes, tag = "3")]
    pub data: Vec<u8>,
    #[prost(message, repeated, tag = "4")]
    pub metadata: Vec<MessageMetaDataProtobufModel>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MessagesProtobufModel {
    #[prost(message, repeated, tag = "1")]
    pub messages: Vec<MessageProtobufModel>,
}

impl MessagesProtobufModel {
    pub fn parse(payload: &[u8]) -> Self {
        prost::Message::decode(payload).unwrap()
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut encoded_payload: Vec<u8> = Vec::new();
        prost::Message::encode(self, &mut encoded_payload).unwrap();
        encoded_payload
    }
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MessageMetaDataProtobufModel {
    #[prost(string, tag = "1")]
    pub key: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub value: ::prost::alloc::string::String,
}
