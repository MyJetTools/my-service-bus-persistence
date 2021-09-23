use my_service_bus_shared::{bcl::BclToUnixMicroseconds, protobuf_models::MessageProtobufModel};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct GetMessagesResponseModel {
    result: i32,
    data: Vec<MessageJsonModel>,
}

impl GetMessagesResponseModel {
    pub fn create(messages: Vec<&MessageProtobufModel>) -> Self {
        let mut data = Vec::new();

        for msg in messages {
            data.push(MessageJsonModel::new(msg))
        }

        GetMessagesResponseModel {
            result: 0, //TODO - засинкать как это сделано с C#
            data,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GetMessageResponseModel {
    result: i32,
    data: Option<MessageJsonModel>,
}

impl GetMessageResponseModel {
    pub fn create(message: Option<&MessageProtobufModel>) -> GetMessageResponseModel {
        GetMessageResponseModel {
            result: 0,
            //TODO - засинкать как это сделано с C#
            data: MessageJsonModel::new_optional(message),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MessageJsonModel {
    id: i64,
    created: String,
    content: String,
}

impl MessageJsonModel {
    pub fn new_optional(src: Option<&MessageProtobufModel>) -> Option<Self> {
        if src.is_none() {
            return None;
        }

        Some(MessageJsonModel::new(src.unwrap()))
    }

    pub fn new(src: &MessageProtobufModel) -> Self {
        let created = match src.created {
            Some(result) => result.to_rfc3339(),
            None => "???".to_string(),
        };
        let result = Self {
            id: src.message_id,
            content: base64::encode(src.data.as_slice()),
            created,
        };

        result
    }
}
