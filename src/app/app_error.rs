use my_azure_storage_sdk::AzureStorageError;
use zip::result::ZipError;

#[derive(Debug)]
pub enum AppError {
    AzureStorageError(AzureStorageError),
    ZipError(ZipError),
}

impl From<AzureStorageError> for AppError {
    fn from(err: AzureStorageError) -> Self {
        AppError::AzureStorageError(err)
    }
}

impl From<ZipError> for AppError {
    fn from(err: ZipError) -> Self {
        AppError::ZipError(err)
    }
}
