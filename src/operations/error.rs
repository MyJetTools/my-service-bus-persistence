use zip::result::ZipError;

use crate::message_pages::PageOperationError;

#[derive(Debug)]
pub enum OperationError {
    TopicNotFound(String),
    PageOperationError(PageOperationError),
    ProtobufDecodeError(prost::DecodeError),
    ProtobufEncodeError(prost::EncodeError),
    ZipError(ZipError),
}

impl From<PageOperationError> for OperationError {
    fn from(src: PageOperationError) -> Self {
        Self::PageOperationError(src)
    }
}

impl From<prost::DecodeError> for OperationError {
    fn from(src: prost::DecodeError) -> Self {
        Self::ProtobufDecodeError(src)
    }
}

impl From<prost::EncodeError> for OperationError {
    fn from(src: prost::EncodeError) -> Self {
        Self::ProtobufEncodeError(src)
    }
}

impl From<ZipError> for OperationError {
    fn from(src: ZipError) -> Self {
        Self::ZipError(src)
    }
}
