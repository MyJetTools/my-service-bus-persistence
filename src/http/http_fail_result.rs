use hyper::{Body, Response};
use my_azure_storage_sdk::AzureStorageError;

use crate::operations::OperationError;

use super::web_content_type::WebContentType;

pub struct HttpFailResult {
    pub status_code: u16,
    content_type: WebContentType,
    content: Vec<u8>,
}

impl HttpFailResult {
    pub fn not_found(msg: String) -> Self {
        Self {
            content_type: WebContentType::Text,
            status_code: 404,
            content: msg.into_bytes(),
        }
    }

    pub fn not_initialized() -> Self {
        Self {
            content_type: WebContentType::Text,
            status_code: 301,
            content: "Application is not initialized".to_string().into_bytes(),
        }
    }

    pub fn query_parameter_requires(param_name: &str) -> Self {
        Self {
            content_type: WebContentType::Text,
            content: format!("Query parameter '{}' is required", param_name).into_bytes(),
            status_code: 301,
        }
    }

    pub fn not_authorized(message: &str) -> Self {
        Self {
            content_type: WebContentType::Text,
            content: message.to_string().into_bytes(),
            status_code: 301,
        }
    }

    pub fn error(message: String) -> Self {
        Self {
            content_type: WebContentType::Text,
            content: message.into_bytes(),
            status_code: 500,
        }
    }
}

impl From<AzureStorageError> for HttpFailResult {
    fn from(src: AzureStorageError) -> Self {
        Self {
            content_type: WebContentType::Text,
            status_code: 404,
            content: format!("{:?}", src).into_bytes(),
        }
    }
}

impl Into<Response<Body>> for HttpFailResult {
    fn into(self) -> Response<Body> {
        Response::builder()
            .header("Content-Type", self.content_type.to_string())
            .status(self.status_code)
            .body(Body::from(self.content))
            .unwrap()
    }
}

impl From<OperationError> for HttpFailResult {
    fn from(src: OperationError) -> Self {
        match src {
            OperationError::TopicNotFound(topic_id) => {
                HttpFailResult::not_found(format!("Topic {} not found", topic_id))
            }
            OperationError::PageOperationError(err) => {
                HttpFailResult::error(format!("ERR:{:?}", err))
            }
            OperationError::ProtobufEncodeError(err) => {
                HttpFailResult::error(format!("ERR:{:?}", err))
            }
            OperationError::ZipError(err) => HttpFailResult::error(format!("ERR:{:?}", err)),
            OperationError::ReadCompressedPageError(err) => {
                HttpFailResult::error(format!("ERR:{:?}", err))
            }
            OperationError::ProtobufDecodeError(err) => {
                HttpFailResult::error(format!("ERR:{:?}", err))
            }
            OperationError::RestoreCompressedPageError(err) => {
                HttpFailResult::error(format!("ERR:{:?}", err))
            }
        }
    }
}
