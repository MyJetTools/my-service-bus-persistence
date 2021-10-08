use prost::EncodeError;
use zip::result::ZipError;

use crate::{compressed_pages::ReadCompressedPageError, message_pages::PageOperationError};

#[derive(Debug)]
pub enum OperationError {
    TopicNotFound(String),
    PageOperationError(PageOperationError),
    ProtobufEncodeError(EncodeError),
    ZipError(ZipError),
    ReadCompressedPageError(ReadCompressedPageError),
    Other(String),
}

impl From<ReadCompressedPageError> for OperationError {
    fn from(src: ReadCompressedPageError) -> Self {
        Self::ReadCompressedPageError(src)
    }
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
