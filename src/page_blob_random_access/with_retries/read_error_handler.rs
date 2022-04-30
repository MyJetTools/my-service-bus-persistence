use std::time::Duration;

use my_azure_storage_sdk::{page_blob::AzurePageBlobStorage, AzureStorageError};

pub enum RetryResult {
    Retry,
    RetryWithDelay(Duration),
    PanicIt,
}

pub async fn is_error_retrieable(
    page_blob: &AzurePageBlobStorage,
    err: &AzureStorageError,
    create_if_not_exists_init_pages_amount: Option<usize>,
    process: &str,
    attempt_no: usize,
) {
    let result = match err {
        AzureStorageError::ContainerNotFound => {
            if let Some(init_pages_amount) = create_if_not_exists_init_pages_amount {
                page_blob.create_container_if_not_exist().await.unwrap();
                page_blob
                    .create_if_not_exists(init_pages_amount)
                    .await
                    .unwrap();
                RetryResult::Retry
            } else {
                RetryResult::PanicIt
            }
        }
        AzureStorageError::BlobNotFound => {
            if let Some(init_pages_amount) = create_if_not_exists_init_pages_amount {
                page_blob
                    .create_if_not_exists(init_pages_amount)
                    .await
                    .unwrap();
                RetryResult::Retry
            } else {
                RetryResult::PanicIt
            }
        }
        AzureStorageError::BlobAlreadyExists => RetryResult::PanicIt,
        AzureStorageError::ContainerBeingDeleted => {
            RetryResult::RetryWithDelay(Duration::from_secs(1))
        }
        AzureStorageError::ContainerAlreadyExists => RetryResult::PanicIt,
        AzureStorageError::InvalidPageRange => RetryResult::RetryWithDelay(Duration::from_secs(1)),
        AzureStorageError::RequestBodyTooLarge => RetryResult::PanicIt,
        AzureStorageError::UnknownError { msg: _ } => RetryResult::PanicIt,
        AzureStorageError::HyperError(_) => RetryResult::RetryWithDelay(Duration::from_secs(1)),
        AzureStorageError::IoError(_) => RetryResult::RetryWithDelay(Duration::from_secs(1)),
    };

    let panic_it = match result {
        RetryResult::Retry => attempt_no > 5,
        RetryResult::RetryWithDelay(duration) => {
            if attempt_no > 5 {
                true
            } else {
                tokio::time::sleep(duration).await;
                false
            }
        }
        RetryResult::PanicIt => true,
    };

    if panic_it {
        panic!("[Attempt:{}] {}. Err: {:?}", attempt_no, process, err);
    }
}
