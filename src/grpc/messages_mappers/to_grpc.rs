use my_service_bus_shared::protobuf_models::{MessageMetaDataProtobufModel, MessageProtobufModel};

use crate::persistence_grpc::*;

pub fn to_message(src: &MessageProtobufModel) -> MessageContentGrpcModel {
    MessageContentGrpcModel {
        data: src.data.clone(),
        created: src.created,
        message_id: src.message_id,
        meta_data: src
            .headers
            .iter()
            .map(|itm| to_message_metadata(itm))
            .collect(),
    }
}

pub fn to_message_metadata(src: &MessageMetaDataProtobufModel) -> MessageContentMetaDataItem {
    MessageContentMetaDataItem {
        key: src.key.to_string(),
        value: src.value.to_string(),
    }
}

impl Into<MessageContentMetaDataItem> for MessageMetaDataProtobufModel {
    fn into(self) -> MessageContentMetaDataItem {
        MessageContentMetaDataItem {
            key: self.key,
            value: self.value,
        }
    }
}
