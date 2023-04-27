use my_azure_storage_sdk::{
    blob_container::BlobContainersApi, AzureStorageConnection, AzureStorageError,
};

pub async fn create_container_if_not_exists(
    connection: &AzureStorageConnection,
    topic_folder: &str,
) {
    let mut attempt_no = 0;
    loop {
        match connection.create_container_if_not_exist(topic_folder).await {
            Ok(_) => {
                return;
            }
            Err(err) => {
                handle_retry_error(err, attempt_no).unwrap();
                attempt_no += 1;
            }
        }
    }
}

fn handle_retry_error(err: AzureStorageError, attempt_no: usize) -> Result<(), AzureStorageError> {
    match err {
        my_azure_storage_sdk::AzureStorageError::ContainerNotFound => {
            return Err(err);
        }
        my_azure_storage_sdk::AzureStorageError::BlobNotFound => {
            return Err(err);
        }
        my_azure_storage_sdk::AzureStorageError::BlobAlreadyExists => {
            return Err(err);
        }
        my_azure_storage_sdk::AzureStorageError::ContainerBeingDeleted => {
            return Err(err);
        }
        my_azure_storage_sdk::AzureStorageError::ContainerAlreadyExists => {}
        my_azure_storage_sdk::AzureStorageError::InvalidPageRange => return Err(err),
        my_azure_storage_sdk::AzureStorageError::RequestBodyTooLarge => return Err(err),
        AzureStorageError::IoError(res) => {
            return Err(AzureStorageError::IoError(res));
        }
        my_azure_storage_sdk::AzureStorageError::HyperError(_) => {
            return Err(err);
        }
        my_azure_storage_sdk::AzureStorageError::Timeout => {}
        my_azure_storage_sdk::AzureStorageError::InvalidResourceName => {}
        AzureStorageError::UnknownError { msg } => {
            return Err(AzureStorageError::UnknownError { msg });
        }
    }

    if attempt_no > 5 {
        return Err(AzureStorageError::UnknownError {
            msg: "Exceeded amount of attempts".to_string(),
        });
    }

    Ok(())
}
