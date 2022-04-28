use std::{collections::HashMap, sync::Arc, usize};

use my_azure_page_blob::MyAzurePageBlob;
use my_azure_page_blob_append::page_blob_utils::get_pages_amount_by_size;
use my_azure_page_blob_random_access::{PageBlobRandomAccess, PageBlobRandomAccessError};
use my_azure_storage_sdk::{
    page_blob::consts::BLOB_PAGE_SIZE, AzureStorageConnection, AzureStorageError,
};

use crate::index_by_minute::utils::{INDEX_STEP, MINUTE_INDEX_BLOB_SIZE};

use super::utils::MinuteWithinYear;

pub struct IndexByMinuteAzurePageBlob {
    pub topic_id: String,
    page_access: HashMap<i32, PageBlobRandomAccess<MyAzurePageBlob>>,
    connection: Arc<AzureStorageConnection>,
}

impl IndexByMinuteAzurePageBlob {
    pub fn new(topic_id: &str, connection: Arc<AzureStorageConnection>) -> Self {
        Self {
            topic_id: topic_id.to_string(),
            page_access: HashMap::new(),
            connection: connection,
        }
    }
    fn get_page_access_by_year(&mut self, year: i32) -> &mut PageBlobRandomAccess<MyAzurePageBlob> {
        if self.page_access.contains_key(&year) {
            return self.page_access.get_mut(&year).unwrap();
        }

        self.page_access.insert(
            year,
            PageBlobRandomAccess::new(
                MyAzurePageBlob::new(
                    self.connection.clone(),
                    self.topic_id.to_string(),
                    get_blob_name(year),
                ),
                false,
                4000,
            ),
        );

        return self.page_access.get_mut(&year).unwrap();
    }

    pub async fn save(
        &mut self,
        year: i32,
        minute: MinuteWithinYear,
        message_id: i64,
    ) -> Result<(), AzureStorageError> {
        let start_pos = get_index_offset(minute);

        let page_access = self.get_page_access_by_year(year);

        loop {
            let result = page_access
                .write(start_pos, &message_id.to_le_bytes())
                .await;

            if result.is_ok() {
                return Ok(());
            }

            if let Err(err) = result {
                match err {
                    PageBlobRandomAccessError::IndexRangeViolation(err) => {
                        panic!("Index range violation error: {}", err);
                    }
                    PageBlobRandomAccessError::AzureStorageError(err) => match err {
                        AzureStorageError::BlobNotFound => {
                            let pages =
                                get_pages_amount_by_size(MINUTE_INDEX_BLOB_SIZE, BLOB_PAGE_SIZE);
                            page_access.create_new(pages).await?;
                        }
                        _ => return Err(err),
                    },
                }
            }
        }
    }

    pub async fn get(
        &mut self,
        year: i32,
        minute: MinuteWithinYear,
    ) -> Result<i64, AzureStorageError> {
        let start_pos = get_index_offset(minute);

        let page_access = self.get_page_access_by_year(year);

        let mut buffer = [0u8; INDEX_STEP];

        let result = page_access.read(start_pos, &mut buffer).await;

        if let Ok(()) = result {
            let result = i64::from_le_bytes(buffer);

            return Ok(result);
        }

        let err = result.err().unwrap();

        if let AzureStorageError::BlobNotFound = err {
            let pages = get_pages_amount_by_size(MINUTE_INDEX_BLOB_SIZE, BLOB_PAGE_SIZE);
            page_access.create_new(pages).await?;
            return Ok(0);
        }

        Err(err)
    }
}

fn get_index_offset(value: MinuteWithinYear) -> usize {
    value.minute as usize * INDEX_STEP
}

fn get_blob_name(year: i32) -> String {
    format!("index-{}", year)
}
