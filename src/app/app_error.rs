use my_azure_page_blob_append::PageBlobAppendError;
use my_azure_storage_sdk::AzureStorageError;
use zip::result::ZipError;

#[derive(Debug)]
pub enum AppError {
    AzureStorageError(AzureStorageError),
    PageBlobAppendError(PageBlobAppendError),
    ZipError(ZipError),
}

impl From<AzureStorageError> for AppError {
    fn from(err: AzureStorageError) -> Self {
        AppError::AzureStorageError(err)
    }
}

impl From<PageBlobAppendError> for AppError {
    fn from(err: PageBlobAppendError) -> Self {
        AppError::PageBlobAppendError(err)
    }
}

impl From<ZipError> for AppError {
    fn from(err: ZipError) -> Self {
        AppError::ZipError(err)
    }
}
