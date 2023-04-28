use std::time::Duration;

use my_azure_page_blob_random_access::PageBlobRandomAccess;
use my_azure_storage_sdk::{page_blob::AzurePageBlobStorage, AzureStorageError};

const DELAY_BETWEEN_RETRIES: Duration = Duration::from_secs(1);

const MAX_ATTEMPTS_AMOUNT: usize = 3;

#[async_trait::async_trait]
pub trait AzurePageBlobStorageWithRetries {
    async fn get_blob_size_or_create_page_blob(
        &self,
        pages_amount_if_create: usize,
    ) -> Result<usize, AzureStorageError>;

    async fn get_blob_size_with_retires(&self) -> Result<usize, AzureStorageError>;

    async fn resize_with_retries(&self, size_in_pages: usize) -> Result<(), AzureStorageError>;

    async fn download_with_retries(&self) -> Result<Vec<u8>, AzureStorageError>;
}

#[async_trait::async_trait]
impl AzurePageBlobStorageWithRetries for AzurePageBlobStorage {
    async fn get_blob_size_or_create_page_blob(
        &self,
        pages_amount_if_create: usize,
    ) -> Result<usize, AzureStorageError> {
        let mut attempt_no = 0;
        loop {
            match self.get_blob_properties().await {
                Ok(result) => {
                    return Ok(result.blob_size);
                }
                Err(err) => {
                    let result =
                        super::handle_error_and_create_blob(self, err, pages_amount_if_create)
                            .await?;

                    if attempt_no >= MAX_ATTEMPTS_AMOUNT {
                        return Err(result);
                    }

                    attempt_no += 1;

                    tokio::time::sleep(DELAY_BETWEEN_RETRIES).await;
                }
            }
        }
    }

    async fn get_blob_size_with_retires(&self) -> Result<usize, AzureStorageError> {
        let mut attempt_no = 0;
        loop {
            match self.get_blob_properties().await {
                Ok(result) => {
                    return Ok(result.blob_size);
                }
                Err(err) => {
                    if attempt_no >= MAX_ATTEMPTS_AMOUNT {
                        return Err(err);
                    }

                    attempt_no += 1;

                    tokio::time::sleep(DELAY_BETWEEN_RETRIES).await;
                }
            }
        }
    }

    async fn download_with_retries(&self) -> Result<Vec<u8>, AzureStorageError> {
        let mut attempt_no = 0;
        loop {
            match self.download().await {
                Ok(result) => {
                    return Ok(result);
                }
                Err(err) => {
                    if attempt_no >= MAX_ATTEMPTS_AMOUNT {
                        return Err(err);
                    }

                    attempt_no += 1;

                    tokio::time::sleep(DELAY_BETWEEN_RETRIES).await;
                }
            }
        }
    }

    async fn resize_with_retries(&self, size_in_pages: usize) -> Result<(), AzureStorageError> {
        let mut attempt_no = 0;
        loop {
            match self.resize(size_in_pages).await {
                Ok(result) => {
                    return Ok(result);
                }
                Err(err) => {
                    if attempt_no >= MAX_ATTEMPTS_AMOUNT {
                        return Err(err);
                    }

                    attempt_no += 1;

                    tokio::time::sleep(DELAY_BETWEEN_RETRIES).await;
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl AzurePageBlobStorageWithRetries for PageBlobRandomAccess {
    async fn get_blob_size_or_create_page_blob(
        &self,
        pages_amount_if_create: usize,
    ) -> Result<usize, AzureStorageError> {
        let mut attempt_no = 0;
        loop {
            match self.get_blob_size().await {
                Ok(result) => {
                    return Ok(result);
                }
                Err(err) => {
                    println!("get_blob_size_or_create_page_blob: Error: {:?}", err);
                    let result =
                        super::handle_error_and_create_blob(self, err, pages_amount_if_create)
                            .await?;

                    if attempt_no >= MAX_ATTEMPTS_AMOUNT {
                        return Err(result);
                    }

                    attempt_no += 1;

                    tokio::time::sleep(DELAY_BETWEEN_RETRIES).await;
                }
            }
        }
    }

    async fn get_blob_size_with_retires(&self) -> Result<usize, AzureStorageError> {
        let mut attempt_no = 0;
        loop {
            match self.get_blob_size().await {
                Ok(result) => {
                    return Ok(result);
                }
                Err(err) => {
                    if attempt_no >= MAX_ATTEMPTS_AMOUNT {
                        return Err(err);
                    }

                    attempt_no += 1;

                    tokio::time::sleep(DELAY_BETWEEN_RETRIES).await;
                }
            }
        }
    }

    async fn download_with_retries(&self) -> Result<Vec<u8>, AzureStorageError> {
        let mut attempt_no = 0;
        loop {
            match self.download().await {
                Ok(result) => {
                    return Ok(result);
                }
                Err(err) => {
                    if attempt_no >= MAX_ATTEMPTS_AMOUNT {
                        return Err(err);
                    }

                    attempt_no += 1;

                    tokio::time::sleep(DELAY_BETWEEN_RETRIES).await;
                }
            }
        }
    }

    async fn resize_with_retries(&self, size_in_pages: usize) -> Result<(), AzureStorageError> {
        let mut attempt_no = 0;
        loop {
            match self.resize(size_in_pages).await {
                Ok(result) => {
                    return Ok(result);
                }
                Err(err) => {
                    if attempt_no >= MAX_ATTEMPTS_AMOUNT {
                        return Err(err);
                    }

                    attempt_no += 1;

                    tokio::time::sleep(DELAY_BETWEEN_RETRIES).await;
                }
            }
        }
    }
}
