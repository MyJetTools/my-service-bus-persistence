use my_service_bus_shared::{
    bcl::BclDateTime,
    protobuf_models::{MessageMetaDataProtobufModel, MessageProtobufModel},
};

use crate::persistence_grpc::*;

pub fn to_message(src: &MessageProtobufModel) -> MessageContentGrpcModel {
    let created: Option<DateTime> = match src.created {
        Some(created) => Some(created.into()),
        None => None,
    };

    MessageContentGrpcModel {
        data: src.data.clone(),
        created,
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

impl Into<DateTime> for BclDateTime {
    fn into(self) -> DateTime {
        DateTime {
            kind: self.kind,
            value: self.value,
            scale: self.scale,
        }
    }
}
