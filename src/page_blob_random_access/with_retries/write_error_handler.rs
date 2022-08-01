use std::time::Duration;

use my_azure_storage_sdk::AzureStorageError;
use rust_extensions::date_time::DateTimeAsMicroseconds;

use super::read_error_handler::RetryResult;

pub async fn is_error_retrieable(
    err: AzureStorageError,
    process: &str,
    attempt_no: usize,
) -> Result<(), AzureStorageError> {
    let result = match &err {
        AzureStorageError::ContainerNotFound => RetryResult::ErrorIt,
        AzureStorageError::BlobNotFound => RetryResult::ErrorIt,

        AzureStorageError::BlobAlreadyExists => RetryResult::ErrorIt,
        AzureStorageError::ContainerBeingDeleted => {
            RetryResult::RetryWithDelay(Duration::from_secs(1))
        }
        my_azure_storage_sdk::AzureStorageError::ContainerAlreadyExists => {
            RetryResult::RetryWithDelay(Duration::from_secs(1))
        }
        my_azure_storage_sdk::AzureStorageError::InvalidPageRange => RetryResult::ErrorIt,
        my_azure_storage_sdk::AzureStorageError::RequestBodyTooLarge => RetryResult::ErrorIt,
        my_azure_storage_sdk::AzureStorageError::UnknownError { msg: _ } => {
            RetryResult::RetryWithDelay(Duration::from_secs(1))
        }
        my_azure_storage_sdk::AzureStorageError::HyperError(_) => {
            RetryResult::RetryWithDelay(Duration::from_secs(1))
        }
        AzureStorageError::IoError(_) => RetryResult::RetryWithDelay(Duration::from_secs(1)),
        AzureStorageError::Timeout => RetryResult::RetryWithDelay(Duration::from_secs(1)),
        AzureStorageError::InvalidResourceName => RetryResult::ErrorIt,
    };

    match result {
        RetryResult::Retry => {
            if attempt_no > 5 {
                println!(
                    "{}: [WriteRetry] Error of process {}",
                    DateTimeAsMicroseconds::now().to_rfc3339(),
                    process
                );
                return Err(err);
            } else {
                return Ok(());
            }
        }
        RetryResult::RetryWithDelay(duration) => {
            if attempt_no > 5 {
                println!(
                    "{}: [WriteRetryWithDelay] Error of process {}",
                    DateTimeAsMicroseconds::now().to_rfc3339(),
                    process
                );
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
