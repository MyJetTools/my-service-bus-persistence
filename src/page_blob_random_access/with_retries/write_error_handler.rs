use std::time::Duration;

use my_azure_storage_sdk::AzureStorageError;

use super::read_error_handler::RetryResult;

pub async fn is_error_retrieable(err: &AzureStorageError, process: &str, attempt_no: usize) {
    let result = match err {
        AzureStorageError::ContainerNotFound => RetryResult::PanicIt,
        AzureStorageError::BlobNotFound => RetryResult::PanicIt,

        AzureStorageError::BlobAlreadyExists => RetryResult::PanicIt,
        AzureStorageError::ContainerBeingDeleted => {
            RetryResult::RetryWithDelay(Duration::from_secs(1))
        }
        my_azure_storage_sdk::AzureStorageError::ContainerAlreadyExists => {
            RetryResult::RetryWithDelay(Duration::from_secs(1))
        }
        my_azure_storage_sdk::AzureStorageError::InvalidPageRange => RetryResult::PanicIt,
        my_azure_storage_sdk::AzureStorageError::RequestBodyTooLarge => RetryResult::PanicIt,
        my_azure_storage_sdk::AzureStorageError::UnknownError { msg: _ } => {
            RetryResult::RetryWithDelay(Duration::from_secs(1))
        }
        my_azure_storage_sdk::AzureStorageError::HyperError(_) => {
            RetryResult::RetryWithDelay(Duration::from_secs(1))
        }
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
