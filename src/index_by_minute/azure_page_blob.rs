use std::{collections::HashMap, usize};

use my_azure_storage_sdk::{page_blob::consts::BLOB_PAGE_SIZE, AzureConnection, AzureStorageError};

use crate::{
    azure_page_blob_writer::{page_blob_utils::get_pages_amount_by_size, PageBlobRandomAccess},
    index_by_minute::utils::{INDEX_STEP, MINUTE_INDEX_BLOB_SIZE},
};

use super::utils::MinuteWithinYear;

pub struct IndexByMinuteAzurePageBlob {
    pub topic_id: String,
    page_access: HashMap<i32, PageBlobRandomAccess>,
    connection: AzureConnection,
}

impl IndexByMinuteAzurePageBlob {
    pub fn new(topic_id: &str, connection: &AzureConnection) -> Self {
        Self {
            topic_id: topic_id.to_string(),
            page_access: HashMap::new(),
            connection: connection.clone(),
        }
    }
    fn get_page_access_by_year(&mut self, year: i32) -> &mut PageBlobRandomAccess {
        if self.page_access.contains_key(&year) {
            return self.page_access.get_mut(&year).unwrap();
        }

        self.page_access.insert(
            year,
            PageBlobRandomAccess::new(
                &self.connection,
                self.topic_id.as_str(),
                &get_blob_name(year),
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
                    AzureStorageError::BlobNotFound => {
                        let pages =
                            get_pages_amount_by_size(MINUTE_INDEX_BLOB_SIZE, BLOB_PAGE_SIZE);
                        page_access.create_new(pages).await?;
                    }
                    _ => return Err(err),
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
