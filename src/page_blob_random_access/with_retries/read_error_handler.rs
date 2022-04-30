use my_azure_storage_sdk::{page_blob::AzurePageBlobStorage, AzureStorageError};

pub async fn is_error_retrieable(
    page_blob: &AzurePageBlobStorage,
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
        AzureStorageError::ContainerAlreadyExists => false,
        AzureStorageError::InvalidPageRange => true,
        AzureStorageError::RequestBodyTooLarge => true,
        AzureStorageError::UnknownError { msg: _ } => true,
        AzureStorageError::HyperError(_) => true,
        AzureStorageError::IoError(_) => true,
    };

    if attempt_no > 5 || !result {
        panic!("[Attempt:{}] {}. Err: {:?}", attempt_no, process, err);
    }

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
}
