use my_azure_storage_sdk::AzureStorageError;
use zip::result::ZipError;

use super::compressed_page::ReadCompressedPageError;

#[derive(Debug)]
pub enum PageOperationError {
    NotInitialized,
    ZipError(ZipError),
    AzureStorageError(AzureStorageError),
    ReadCompressedPageError(ReadCompressedPageError),
}

impl From<ZipError> for PageOperationError {
    fn from(src: ZipError) -> Self {
        Self::ZipError(src)
    }
}

impl From<AzureStorageError> for PageOperationError {
    fn from(src: AzureStorageError) -> Self {
        Self::AzureStorageError(src)
    }
}

impl From<ReadCompressedPageError> for PageOperationError {
    fn from(src: ReadCompressedPageError) -> Self {
        Self::ReadCompressedPageError(src)
    }
}
