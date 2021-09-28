use prost::EncodeError;
use zip::result::ZipError;

use crate::message_pages::PageOperationError;

#[derive(Debug)]
pub enum OperationError {
    TopicNotFound,
    PageOperationError(PageOperationError),
    ProtobufEncodeError(EncodeError),
    ZipError(ZipError),
}

impl From<PageOperationError> for OperationError {
    fn from(src: PageOperationError) -> Self {
        Self::PageOperationError(src)
    }
}

impl From<EncodeError> for OperationError {
    fn from(src: EncodeError) -> Self {
        Self::ProtobufEncodeError(src)
    }
}

impl From<ZipError> for OperationError {
    fn from(src: ZipError) -> Self {
        Self::ZipError(src)
    }
}
