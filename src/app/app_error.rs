use my_azure_storage_sdk::AzureStorageError;

use crate::azure_page_blob_writer::PageBlobAppendCacheError;

#[derive(Debug)]
pub enum AppError {
    AzureStorageError { err: AzureStorageError },
    PageBlobAppendCacheError { err: PageBlobAppendCacheError },
    Other { msg: String },
}

impl From<AzureStorageError> for AppError {
    fn from(err: AzureStorageError) -> Self {
        AppError::AzureStorageError { err }
    }
}

impl From<PageBlobAppendCacheError> for AppError {
    fn from(err: PageBlobAppendCacheError) -> Self {
        AppError::PageBlobAppendCacheError { err }
    }
}
