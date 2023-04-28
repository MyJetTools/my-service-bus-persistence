use my_azure_storage_sdk::{page_blob::PageBlobAbstractions, AzureStorageError};

pub async fn handle_error_and_create_blob(
    page_blob: &impl PageBlobAbstractions,
    err: AzureStorageError,
    init_page_blob_size: usize,
) -> Result<AzureStorageError, AzureStorageError> {
    match err {
        AzureStorageError::ContainerNotFound => {
            println!("Creating container");
            page_blob.create_container_if_not_exists().await?;
            println!("Creating Blob");
            page_blob
                .create_blob_if_not_exists(init_page_blob_size)
                .await?;

            println!("Created container and blob");
            Ok(err)
        }
        AzureStorageError::BlobNotFound => {
            println!("Creating blob");
            page_blob
                .create_blob_if_not_exists(init_page_blob_size)
                .await?;

            println!("Created blob");
            Ok(err)
        }
        _ => Err(err),
    }
}
