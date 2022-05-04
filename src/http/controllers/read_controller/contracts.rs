use std::sync::Arc;

use my_http_server_swagger::{MyHttpInput, MyHttpObjectStructure};
use my_service_bus_shared::{bcl::BclToUnixMicroseconds, protobuf_models::MessageProtobufModel};
use serde::{Deserialize, Serialize};

#[derive(MyHttpInput)]
pub struct GetMessageByIdInputContract {
    #[http_query(name = "topicId"; description="Id of topic")]
    pub topic_id: String,

    #[http_query(name = "messageId"; description="Id of message")]
    pub message_id: i64,
}

#[derive(MyHttpInput)]
pub struct GetMessagesByIdInputContract {
    #[http_query(name = "topicId"; description="Id of topic")]
    pub topic_id: String,

    #[http_query(name = "maxAmount"; description="Maximum amounts to read"; default="1")]
    pub max_amount: usize,

    #[http_query(name = "fromDate"; description="From date")]
    pub from_date: String,
}

#[derive(Serialize, Deserialize, Debug, MyHttpObjectStructure)]
pub struct GetMessagesResponseModel {
    result: i32,
    data: Vec<MessageJsonModel>,
}

impl GetMessagesResponseModel {
    pub fn create(messages: Vec<Arc<MessageProtobufModel>>) -> Self {
        let mut data = Vec::new();

        for msg in &messages {
            data.push(MessageJsonModel::new(msg))
        }

        GetMessagesResponseModel { result: 0, data }
    }
}

#[derive(Serialize, Deserialize, Debug, MyHttpObjectStructure)]
pub struct GetMessageResponseModel {
    result: i32,
    data: MessageJsonModel,
}

impl GetMessageResponseModel {
    pub fn create(message: &MessageProtobufModel) -> GetMessageResponseModel {
        GetMessageResponseModel {
            result: 0,
            data: MessageJsonModel::new(message),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, MyHttpObjectStructure)]
pub struct MessageJsonModel {
    id: i64,
    created: String,
    content: String,
}

impl MessageJsonModel {
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
