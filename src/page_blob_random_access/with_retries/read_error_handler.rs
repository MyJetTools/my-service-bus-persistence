use my_azure_page_blob::{MyAzurePageBlob, MyPageBlob};
use my_azure_storage_sdk::AzureStorageError;

pub async fn is_error_retrieable(
    page_blob: &MyAzurePageBlob,
    err: &AzureStorageError,
    create_if_not_exists_init_size: Option<usize>,
    process: &str,
    attempt_no: usize,
) {
    let result = match err {
        AzureStorageError::ContainerNotFound => {
            if let Some(init_pages_amount) = create_if_not_exists_init_size {
                page_blob.create_container_if_not_exist().await.unwrap();
                page_blob
                    .create_if_not_exists(init_pages_amount)
                    .await
                    .unwrap();
                true
            } else {
                false
            }
        }
        AzureStorageError::BlobNotFound => {
            if let Some(init_pages_amount) = create_if_not_exists_init_size {
                page_blob
                    .create_if_not_exists(init_pages_amount)
                    .await
                    .unwrap();
                true
            } else {
                false
            }
        }
        AzureStorageError::BlobAlreadyExists => false,
        AzureStorageError::ContainerBeingDeleted => true,
        my_azure_storage_sdk::AzureStorageError::ContainerAlreadyExists => false,
        my_azure_storage_sdk::AzureStorageError::InvalidPageRange => true,
        my_azure_storage_sdk::AzureStorageError::RequestBodyTooLarge => true,
        my_azure_storage_sdk::AzureStorageError::UnknownError { msg: _ } => true,
        my_azure_storage_sdk::AzureStorageError::HyperError { err: _ } => true,
    };

    if attempt_no > 5 || !result {
        panic!("[Attempt:{}] {}. Err: {:?}", attempt_no, process, err);
    }

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
}
