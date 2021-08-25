use my_azure_storage_sdk::{AzureConnection, AzureStorageError};

pub async fn copy_blob(
    conn_string: &AzureConnection,
    from_container: &str,
    from_blob: &str,
    to_container: &str,
    to_blob: &str,
    pages_to_copy: usize,
) -> Result<(), AzureStorageError> {
    todo!("Implement")
}
