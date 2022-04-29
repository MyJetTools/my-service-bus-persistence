use my_azure_page_blob::MyAzurePageBlob;
use my_azure_storage_sdk::AzureStorageError;

pub async fn is_error_retrieable(
    page_blob: &MyAzurePageBlob,
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
        my_azure_storage_sdk::AzureStorageError::HyperError { err: _ } => true,
    };

    if attempt_no > 5 || !result {
        panic!("[Attempt:{}] {}. Err: {:?}", attempt_no, process, err);
    }

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
}
