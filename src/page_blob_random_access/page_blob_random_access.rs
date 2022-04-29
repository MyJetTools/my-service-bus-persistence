use my_azure_page_blob::MyAzurePageBlob;
use my_azure_storage_sdk::page_blob::consts::BLOB_PAGE_SIZE;

use super::{LastKnownPageCache, PageBlobPageId, RandomAccessData};

pub struct PageBlobRandomAccess {
    page_blob: MyAzurePageBlob,
    blob_size: Option<usize>,
    last_known_page_cache: LastKnownPageCache,
}

impl PageBlobRandomAccess {
    pub fn new(page_blob: MyAzurePageBlob) -> Self {
        Self {
            page_blob,
            blob_size: None,
            last_known_page_cache: LastKnownPageCache::new(),
        }
    }

    pub async fn get_blob_size(&mut self, create_if_not_exists_init_size: Option<usize>) -> usize {
        if self.blob_size.is_none() {
            let result =
                super::with_retries::get_blob_size(&self.page_blob, create_if_not_exists_init_size)
                    .await;
            self.blob_size = Some(result);
        }

        self.blob_size.unwrap()
    }

    pub async fn save_pages(&mut self, start_page_no: &PageBlobPageId, content: &[u8]) {
        super::with_retries::save_pages(&self.page_blob, start_page_no, content).await;

        self.last_known_page_cache
            .update(start_page_no.value, content);
    }

    pub async fn load_pages(
        &mut self,
        start_page_no: &PageBlobPageId,
        pages_amount: usize,
        create_if_not_exists_init_size: Option<usize>,
    ) -> Vec<u8> {
        if pages_amount == 1 {
            if let Some(content) = self
                .last_known_page_cache
                .get_page_cache_content(start_page_no.value)
            {
                return content.to_vec();
            }
        }

        let result = super::with_retries::read_pages(
            &self.page_blob,
            start_page_no,
            pages_amount,
            create_if_not_exists_init_size,
        )
        .await;

        self.last_known_page_cache
            .update(start_page_no.value, result.as_slice());

        result
    }

    pub async fn resize_blob(&mut self, pages_amount: usize) {
        super::with_retries::resize(&self.page_blob, pages_amount).await;
        self.blob_size = Some(pages_amount * BLOB_PAGE_SIZE);

        if let Some(page_no_in_cache) = self.last_known_page_cache.get_page_no() {
            if page_no_in_cache.value >= pages_amount {
                self.last_known_page_cache.clear();
            }
        }
    }

    pub async fn read_randomly(&mut self, start_pos: usize, len: usize) -> RandomAccessData {
        let mut result = RandomAccessData::new(start_pos, len);

        let payload = self
            .load_pages(&result.get_start_page_id(), result.get_pages_amout(), None)
            .await;

        result.assign_content(payload);

        result
    }

    pub async fn write_randomly(&mut self, start_pos: usize, content: &[u8]) {
        let mut result = RandomAccessData::new(start_pos, content.len());

        let start_page_id = result.get_start_page_id();

        let first_page = if result.get_payload_offset() > 0 {
            let first_page = self.load_pages(&start_page_id, 1, None).await;
            Some(first_page)
        } else {
            None
        };

        result.assign_write_content(first_page, content);

        self.save_pages(&start_page_id, result.get_whole_payload())
            .await;
    }

    pub async fn download(&self, create_if_not_exists_init_size: Option<usize>) -> Vec<u8> {
        super::with_retries::download(&self.page_blob, create_if_not_exists_init_size).await
    }
}
