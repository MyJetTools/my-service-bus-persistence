use my_service_bus_shared::page_compressor::CompressedPageReaderError;
use zip::result::ZipError;

#[derive(Debug)]
pub enum OperationError {
    TopicNotFound(String),

    ProtobufDecodeError(prost::DecodeError),
    ProtobufEncodeError(prost::EncodeError),
    CompressedPageReaderError(CompressedPageReaderError),
    ZipError(ZipError),
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

impl From<CompressedPageReaderError> for OperationError {
    fn from(src: CompressedPageReaderError) -> Self {
        Self::CompressedPageReaderError(src)
    }
}
