use my_azure_page_blob_append::PageBlobAppendCacheError;
use my_azure_storage_sdk::AzureStorageError;
use zip::result::ZipError;

#[derive(Debug)]
pub enum AppError {
    AzureStorageError(AzureStorageError),
    PageBlobAppendCacheError(PageBlobAppendCacheError),
    ZipError(ZipError),
}

impl From<AzureStorageError> for AppError {
    fn from(err: AzureStorageError) -> Self {
        AppError::AzureStorageError(err)
    }
}

impl From<PageBlobAppendCacheError> for AppError {
    fn from(err: PageBlobAppendCacheError) -> Self {
        AppError::PageBlobAppendCacheError(err)
    }
}

impl From<ZipError> for AppError {
    fn from(err: ZipError) -> Self {
        AppError::ZipError(err)
    }
}
