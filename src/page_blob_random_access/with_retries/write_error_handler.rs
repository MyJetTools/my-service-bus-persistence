use my_azure_storage_sdk::{page_blob::AzurePageBlobStorage, AzureStorageError};

pub async fn is_error_retrieable(
    page_blob: &AzurePageBlobStorage,
    err: &AzureStorageError,
    process: &str,
    attempt_no: usize,
) {
    let result = match err {
        AzureStorageError::ContainerNotFound => false,
        AzureStorageError::BlobNotFound => false,

        AzureStorageError::BlobAlreadyExists => false,
        AzureStorageError::ContainerBeingDeleted => true,
        my_azure_storage_sdk::AzureStorageError::ContainerAlreadyExists => false,
        my_azure_storage_sdk::AzureStorageError::InvalidPageRange => false,
        my_azure_storage_sdk::AzureStorageError::RequestBodyTooLarge => false,
        my_azure_storage_sdk::AzureStorageError::UnknownError { msg: _ } => true,
        my_azure_storage_sdk::AzureStorageError::HyperError(_) => true,
        AzureStorageError::IoError(_) => true,
    };

    if attempt_no > 5 || !result {
        panic!("[Attempt:{}] {}. Err: {:?}", attempt_no, process, err);
    }

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
}
