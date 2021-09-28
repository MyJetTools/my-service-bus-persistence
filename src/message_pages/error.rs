use my_azure_storage_sdk::AzureStorageError;
use zip::result::ZipError;

#[derive(Debug)]
pub enum PageOperationError {
    NotInitialized,
    ZipError(ZipError),
    AzureStorageError(AzureStorageError),
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
