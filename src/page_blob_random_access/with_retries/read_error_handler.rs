use std::time::Duration;

use my_azure_storage_sdk::{page_blob::AzurePageBlobStorage, AzureStorageError};

pub enum RetryResult {
    Retry,
    RetryWithDelay(Duration),
    ErrorIt,
}

pub async fn is_error_retrieable(
    page_blob: &AzurePageBlobStorage,
    err: AzureStorageError,
    create_if_not_exists_init_pages_amount: Option<usize>,
    process: &str,
    attempt_no: usize,
) -> Result<(), AzureStorageError> {
    let result = match &err {
        AzureStorageError::ContainerNotFound => {
            if let Some(init_pages_amount) = create_if_not_exists_init_pages_amount {
                super::create_container_if_not_exist(page_blob).await;
                page_blob
                    .create_if_not_exists(init_pages_amount)
                    .await
                    .unwrap();
                RetryResult::Retry
            } else {
                RetryResult::ErrorIt
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
                RetryResult::ErrorIt
            }
        }
        AzureStorageError::BlobAlreadyExists => RetryResult::ErrorIt,
        AzureStorageError::ContainerBeingDeleted => {
            RetryResult::RetryWithDelay(Duration::from_secs(1))
        }
        AzureStorageError::ContainerAlreadyExists => RetryResult::ErrorIt,
        AzureStorageError::InvalidPageRange => RetryResult::RetryWithDelay(Duration::from_secs(1)),
        AzureStorageError::RequestBodyTooLarge => RetryResult::ErrorIt,
        AzureStorageError::UnknownError { msg: _ } => RetryResult::ErrorIt,
        AzureStorageError::HyperError(_) => RetryResult::RetryWithDelay(Duration::from_secs(1)),
        AzureStorageError::IoError(_) => RetryResult::RetryWithDelay(Duration::from_secs(1)),
        AzureStorageError::Timeout => RetryResult::RetryWithDelay(Duration::from_secs(1)),
    };

    match result {
        RetryResult::Retry => {
            if attempt_no > 5 {
                println!("Error of process {}", process);
                return Err(err);
            } else {
                return Ok(());
            }
        }
        RetryResult::RetryWithDelay(duration) => {
            if attempt_no > 5 {
                println!("Error of process {}", process);
                return Err(err);
            } else {
                tokio::time::sleep(duration).await;
                return Ok(());
            }
        }
        RetryResult::ErrorIt => {
            return Err(err);
        }
    };
}
