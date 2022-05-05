use my_azure_storage_sdk::page_blob::consts::BLOB_PAGE_SIZE;

use crate::page_blob_random_access::{PageBlobPageId, PageBlobRandomAccess};

use super::utils::MINUTE_INDEX_FILE_SIZE;
pub static INIT_PAGES_SIZE: usize = MINUTE_INDEX_FILE_SIZE / BLOB_PAGE_SIZE;

pub struct IndexByMinuteStorage {
    page_blob: PageBlobRandomAccess,
}

impl IndexByMinuteStorage {
    pub fn new(page_blob: PageBlobRandomAccess) -> Self {
        Self { page_blob }
    }

    pub async fn load(&mut self) -> Vec<u8> {
        let file_size = self.page_blob.get_blob_size(Some(INIT_PAGES_SIZE)).await;

        if file_size < MINUTE_INDEX_FILE_SIZE {
            self.page_blob.resize_blob(INIT_PAGES_SIZE).await;
            return vec![0u8; MINUTE_INDEX_FILE_SIZE];
        }

        self.page_blob.download(Some(INIT_PAGES_SIZE)).await
    }

    pub async fn save(&mut self, content: &[u8], page_no: &PageBlobPageId) {
        self.page_blob.save_pages(page_no, content).await;
    }
}
