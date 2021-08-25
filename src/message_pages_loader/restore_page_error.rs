use my_azure_storage_sdk::AzureStorageError;

pub enum RestorePageError {
    AzureStorageError(AzureStorageError),
}

impl From<AzureStorageError> for RestorePageError {
    fn from(src: AzureStorageError) -> Self {
        Self::AzureStorageError(src)
    }
}
