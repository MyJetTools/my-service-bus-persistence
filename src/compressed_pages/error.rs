use my_azure_storage_sdk::AzureStorageError;
use zip::result::ZipError;

#[derive(Debug)]
pub enum ReadCompressedPageError {
    ZipError(ZipError),
    AzureStorageError(AzureStorageError),
    ProtobufDecodeError(prost::DecodeError),
    ProtobufEncodeError(prost::EncodeError),
}

impl From<ZipError> for ReadCompressedPageError {
    fn from(src: ZipError) -> Self {
        Self::ZipError(src)
    }
}

impl From<AzureStorageError> for ReadCompressedPageError {
    fn from(src: AzureStorageError) -> Self {
        Self::AzureStorageError(src)
    }
}

impl From<prost::DecodeError> for ReadCompressedPageError {
    fn from(src: prost::DecodeError) -> Self {
        Self::ProtobufDecodeError(src)
    }
}

impl From<prost::EncodeError> for ReadCompressedPageError {
    fn from(src: prost::EncodeError) -> Self {
        Self::ProtobufEncodeError(src)
    }
}
