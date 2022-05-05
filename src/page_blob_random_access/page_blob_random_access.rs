use my_azure_storage_sdk::page_blob::{consts::BLOB_PAGE_SIZE, AzurePageBlobStorage};

use super::{LastKnownPageCache, PageBlobPageId, RandomAccessData};

pub struct PageBlobRandomAccess {
    pub page_blob: AzurePageBlobStorage,
    blob_size: Option<usize>,
    last_known_page_cache: LastKnownPageCache,
    max_pages_amount_chunk: usize,
}

impl PageBlobRandomAccess {
    pub async fn open_if_exists(
        page_blob: AzurePageBlobStorage,
        max_pages_amount_chunk: usize,
    ) -> Option<Self> {
        let blob_size = super::with_retries::get_blob_properties(&page_blob).await?;

        Self {
            page_blob,
            blob_size: Some(blob_size),
            last_known_page_cache: LastKnownPageCache::new(),
            max_pages_amount_chunk,
        }
        .into()
    }

    pub async fn open_or_create(
        page_blob: AzurePageBlobStorage,
        max_pages_amount_chunk: usize,
    ) -> Self {
        page_blob.create_container_if_not_exist().await.unwrap();

        Self {
            page_blob,
            blob_size: None,
            last_known_page_cache: LastKnownPageCache::new(),
            max_pages_amount_chunk,
        }
    }

    pub async fn get_blob_size(
        &mut self,
        create_if_not_exists_init_size_in_pages: Option<usize>,
    ) -> usize {
        if self.blob_size.is_none() {
            let result = super::with_retries::get_blob_size(
                &self.page_blob,
                create_if_not_exists_init_size_in_pages,
            )
            .await;
            self.blob_size = Some(result);
        }

        self.blob_size.unwrap()
    }

    pub async fn get_pages_amount(
        &mut self,
        create_if_not_exists_init_size: Option<usize>,
    ) -> usize {
        self.get_blob_size(create_if_not_exists_init_size).await / BLOB_PAGE_SIZE
    }

    pub async fn save_pages(&mut self, start_page_no: &PageBlobPageId, content: &[u8]) {
        super::with_retries::save_pages(
            &self.page_blob,
            start_page_no,
            content,
            self.max_pages_amount_chunk,
        )
        .await;

        self.last_known_page_cache
            .update(start_page_no.value, content);
    }

    pub async fn load_pages(
        &mut self,
        start_page_no: &PageBlobPageId,
        pages_amount: usize,
        create_if_not_exists_init_pages_amount: Option<usize>,
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
            create_if_not_exists_init_pages_amount,
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

    pub async fn read_from_position(&mut self, start_pos: usize, len: usize) -> RandomAccessData {
        let mut result = RandomAccessData::new(start_pos, len);

        let payload = self
            .load_pages(&result.get_start_page_id(), result.get_pages_amout(), None)
            .await;

        result.assign_content(payload);

        result
    }

    pub async fn write_at_position(
        &mut self,
        start_pos: usize,
        content: &[u8],
        auto_resize_rate_in_pages: usize,
    ) {
        let mut payload_to_write = RandomAccessData::new(start_pos, content.len());

        let start_page_id = payload_to_write.get_start_page_id();

        let first_page = if payload_to_write.get_payload_offset() > 0 {
            let first_page = self.load_pages(&start_page_id, 1, None).await;
            Some(first_page)
        } else {
            None
        };

        payload_to_write.assign_write_content(first_page, content);

        payload_to_write.make_payload_size_complient();

        let required_pages_amount_after_we_write =
            super::utils::calc_required_pages_amount_after_we_append(
                start_pos + content.len(),
                auto_resize_rate_in_pages,
                BLOB_PAGE_SIZE,
            );

        if self.get_pages_amount(Some(auto_resize_rate_in_pages)).await
            < required_pages_amount_after_we_write
        {
            self.resize_blob(required_pages_amount_after_we_write).await;
        }

        self.save_pages(&start_page_id, payload_to_write.get_whole_payload())
            .await;
    }

    pub async fn download(&mut self, create_if_not_exists_init_size: Option<usize>) -> Vec<u8> {
        let result =
            super::with_retries::download(&self.page_blob, create_if_not_exists_init_size).await;

        self.blob_size = Some(result.len());
        result
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use my_azure_storage_sdk::AzureStorageConnection;

    use super::*;

    use my_azure_storage_sdk::page_blob::AzurePageBlobStorage;

    #[tokio::test]
    pub async fn test_we_have_everything_in_one_page() {
        let connection = AzureStorageConnection::new_in_memory();
        let azure_page_blob =
            AzurePageBlobStorage::new(Arc::new(connection), "test".to_string(), "test".to_string())
                .await;
        let mut page_blob_random_access =
            PageBlobRandomAccess::open_or_create(azure_page_blob, 1024).await;

        let mut start_pos = 0;
        let mut end_pos = 2;

        for _ in 1..15 {
            let mut save = Vec::new();
            for b in start_pos..end_pos {
                save.push((b + 1) as u8);
            }

            page_blob_random_access
                .write_at_position(start_pos, save.as_slice(), 1)
                .await;

            let result = page_blob_random_access.read_from_position(0, end_pos).await;

            let mut res_to_compare = Vec::new();
            for b in 0..end_pos {
                res_to_compare.push((b + 1) as u8);
            }

            assert_eq!(result.as_slice(), res_to_compare.as_slice());

            let old_end_pos = end_pos;

            start_pos = end_pos;
            end_pos = start_pos + old_end_pos + 1;
        }
    }

    #[tokio::test]
    async fn test_random_read_write() {
        let connection = AzureStorageConnection::new_in_memory();
        let page_blob =
            AzurePageBlobStorage::new(Arc::new(connection), "test".to_string(), "test".to_string())
                .await;

        let mut page_blob_random_access =
            PageBlobRandomAccess::open_or_create(page_blob, 1024).await;

        let first_page = [1u8; 512];

        page_blob_random_access
            .write_at_position(0, &first_page, 1)
            .await;

        assert_eq!(
            first_page.as_slice(),
            page_blob_random_access
                .last_known_page_cache
                .get_page_cache_content(0)
                .unwrap()
        );

        assert_eq!(
            0,
            page_blob_random_access
                .last_known_page_cache
                .get_page_no()
                .unwrap()
                .value
        );

        let first_page = [2u8; 512];

        page_blob_random_access
            .write_at_position(1, &first_page, 1)
            .await;

        let first_page = [3u8; 512];

        page_blob_random_access
            .write_at_position(0, &first_page, 1)
            .await;
    }
}
