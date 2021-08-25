use std::usize;

use my_azure_storage_sdk::{page_blob::consts::BLOB_PAGE_SIZE, AzureStorageError};

use crate::azure_page_blob_writer::page_blob_utils;
use my_azure_page_blob::*;

use super::page_blob_buffer::PageBlobBuffer;

#[derive(Debug)]
pub enum PageBlobAppendCacheError {
    NotInitialized,
    MaxSizeProtection { limit: usize, size_from_blob: usize },
    AzureStorageError(AzureStorageError),
}

impl From<AzureStorageError> for PageBlobAppendCacheError {
    fn from(err: AzureStorageError) -> Self {
        Self::AzureStorageError(err)
    }
}

pub struct PageBlobAppendCache {
    blob_size_in_pages: Option<usize>,
    pages_have_read: usize,
    pub blob_position: usize,
    buffer: PageBlobBuffer,
    max_payload_size_protection: usize,
    pub blob_scanned: bool,
    blob_auto_resize_in_pages: usize,
    auto_create_container: bool,
}

impl PageBlobAppendCache {
    pub fn new(
        cache_capacity_in_pages: usize,
        blob_auto_resize_in_pages: usize,
        max_payload_size_protection: usize,
        auto_create_container: bool,
    ) -> Self {
        Self {
            blob_size_in_pages: None,
            pages_have_read: 0,
            blob_position: 0,
            max_payload_size_protection,
            blob_scanned: false,
            blob_auto_resize_in_pages,
            auto_create_container,
            buffer: PageBlobBuffer::new(BLOB_PAGE_SIZE, cache_capacity_in_pages),
        }
    }

    async fn handle_init_blob_err<T: MyPageBlob>(
        &mut self,
        page_blob: &mut T,
        err: AzureStorageError,
    ) -> Result<(), PageBlobAppendCacheError> {
        match err {
            AzureStorageError::ContainerNotFound => {
                if self.auto_create_container {
                    page_blob.create_container_if_not_exist().await?;
                    return Ok(());
                }
            }

            _ => {}
        }

        return Err(PageBlobAppendCacheError::AzureStorageError(err));
    }

    async fn init<T: MyPageBlob>(
        &mut self,
        page_blob: &mut T,
    ) -> Result<usize, PageBlobAppendCacheError> {
        if let Some(result) = self.blob_size_in_pages {
            return Ok(result);
        }

        loop {
            let result = page_blob.get_available_pages_amount().await;

            match result {
                Ok(available_pages) => {
                    self.blob_size_in_pages = Some(available_pages);
                    return Ok(available_pages);
                }
                Err(err) => {
                    self.handle_init_blob_err(page_blob, err).await?;
                }
            }
        }
    }

    pub async fn get_next_payload<T: MyPageBlob>(
        &mut self,
        page_blob: &mut T,
    ) -> Result<Option<Vec<u8>>, PageBlobAppendCacheError> {
        self.init(page_blob).await?;

        loop {
            let data_size = self.buffer.get_next_message_size();

            if data_size.is_none() {
                let data_size_to_load = self.buffer.get_buffer_size_to_append();

                let pages_amount_to_load = super::page_blob_utils::get_pages_amount_by_size(
                    data_size_to_load,
                    BLOB_PAGE_SIZE,
                );
                let data = self
                    .load_next_pages(page_blob, pages_amount_to_load)
                    .await?;

                if data.is_none() {
                    return Ok(None);
                }

                self.buffer.append(data.unwrap().as_slice());
                continue;
            }

            let data_size = data_size.unwrap();

            if data_size > self.max_payload_size_protection {
                return Err(PageBlobAppendCacheError::MaxSizeProtection {
                    limit: self.max_payload_size_protection,
                    size_from_blob: data_size,
                });
            }

            if data_size == 0 {
                self.blob_scanned = true;
                return Ok(None);
            }

            let payload = self.get_payload(page_blob, data_size).await?;

            if payload.is_none() {
                return Ok(None);
            }

            let payload = payload.unwrap();

            return Ok(Some(payload));
        }
    }

    async fn get_payload<T: MyPageBlob>(
        &mut self,
        page_blob: &mut T,
        data_size: usize,
    ) -> Result<Option<Vec<u8>>, PageBlobAppendCacheError> {
        let payload_result = self.buffer.get_payload(data_size);
        self.blob_position += 4 + data_size;

        if let Some(payload) = payload_result {
            return Ok(Some(payload.to_vec()));
        }

        let mut remain_size = data_size;
        let mut result_buffer: Vec<u8> = Vec::with_capacity(data_size);

        let remaining_payload = self.buffer.get_remaining_payload();

        result_buffer.extend(remaining_payload);
        remain_size -= remaining_payload.len();

        let remain_pages_amount_to_read =
            page_blob_utils::get_pages_amount_by_size_including_buffer_capacity(
                remain_size,
                self.buffer.capacity(),
                BLOB_PAGE_SIZE,
            );

        let data_from_blob = self
            .load_next_pages(page_blob, remain_pages_amount_to_read)
            .await?;

        if data_from_blob.is_none() {
            return Ok(None);
        }

        let data_from_blob = data_from_blob.unwrap();

        page_blob_utils::extend_result_buffer_and_cahce(
            &mut result_buffer,
            data_from_blob.as_slice(),
            remain_size,
            &mut self.buffer,
            self.blob_position,
            BLOB_PAGE_SIZE,
        );

        return Ok(Some(result_buffer));
    }

    async fn load_next_pages<T: MyPageBlob>(
        &mut self,
        page_blob: &mut T,
        pages_amount_to_read: usize,
    ) -> Result<Option<Vec<u8>>, PageBlobAppendCacheError> {
        self.init(page_blob).await?;

        let blob_size_in_pages = self.blob_size_in_pages.unwrap();

        let mut pages_amount_to_read = pages_amount_to_read;

        if pages_amount_to_read + self.pages_have_read > blob_size_in_pages {
            pages_amount_to_read = blob_size_in_pages - self.pages_have_read;
        }

        if pages_amount_to_read == 0 {
            return Ok(None);
        }

        let result = page_blob
            .get(self.pages_have_read, pages_amount_to_read)
            .await;

        if let Err(err) = result {
            return Err(PageBlobAppendCacheError::AzureStorageError(err));
        }

        let result = result.unwrap();
        self.pages_have_read += pages_amount_to_read;
        return Ok(Some(result));
    }

    async fn create_new_blob<TMyPageBlob: MyPageBlob>(
        &mut self,
        page_blob: &mut TMyPageBlob,
    ) -> Result<(), AzureStorageError> {
        page_blob.create(0).await?;
        self.blob_size_in_pages = Some(0);
        self.blob_scanned = true;
        Ok(())
    }

    async fn check_init_status_for_write_operation<T: MyPageBlob>(
        &mut self,
        page_blob: &mut T,
    ) -> Result<(), PageBlobAppendCacheError> {
        if self.blob_size_in_pages.is_none() {
            self.create_new_blob(page_blob).await?;
        }

        if !self.blob_scanned {
            return Err(PageBlobAppendCacheError::NotInitialized);
        }

        if self.blob_size_in_pages.is_none() {
            page_blob
                .create_if_not_exists(self.blob_auto_resize_in_pages)
                .await?;

            self.init(page_blob).await?;
        }

        Ok(())
    }

    pub async fn append_and_write<'s, T: MyPageBlob>(
        &mut self,
        page_blob: &mut T,
        payloads: &Vec<Vec<u8>>,
    ) -> Result<(), PageBlobAppendCacheError> {
        self.check_init_status_for_write_operation(page_blob)
            .await?;

        let payload = page_blob_utils::compile_payloads(&payloads);
        let payload_len = payload.len();

        let mut payload_to_upload = self.buffer.get_last_page().to_vec();

        payload_to_upload.extend(&payload);

        let last_page = page_blob_utils::get_last_page(&payload_to_upload, BLOB_PAGE_SIZE).to_vec();

        payload_to_upload.extend(&[0u8, 0u8, 0u8, 0u8]);

        page_blob_utils::extend_buffer_to_full_pages_size(&mut payload_to_upload, BLOB_PAGE_SIZE);

        //---------------------
        let page_no = page_blob_utils::get_page_no_from_page_blob_position(
            self.blob_position,
            BLOB_PAGE_SIZE,
        );

        page_blob
            .auto_ressize_and_save_pages(page_no, payload_to_upload, self.blob_auto_resize_in_pages)
            .await?;

        self.blob_position += payload_len;

        //----------------------------------------

        self.buffer.set_last_page(last_page.as_slice());

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use my_azure_page_blob::*;

    use super::*;

    fn assert_bytes(left: &[u8], right: &[u8]) {
        assert_eq!(left.len(), right.len());

        for i in 0..left.len() {
            let left_b = left[i];
            let right_b = right[i];

            if left_b != right_b {
                println!("Not the same numbers at position: {}", i);
            }
            assert_eq!(left_b, right_b);
        }
    }

    fn compile_payload(b: u8, amount: usize) -> Vec<u8> {
        let mut result = Vec::with_capacity(amount);
        for _ in 0..amount {
            result.push(b);
        }

        result
    }

    impl PageBlobAppendCache {
        pub async fn init_for_tests<T: MyPageBlob>(&mut self, page_blob: &mut T) {
            loop {
                let msg = self.get_next_payload(page_blob).await;

                if let Err(err) = &msg {
                    if let PageBlobAppendCacheError::AzureStorageError(err) = err {
                        if let AzureStorageError::BlobNotFound = err {
                            page_blob.create_if_not_exists(0).await.unwrap();
                            return;
                        }
                    }
                }

                let msg = msg.unwrap();

                if msg.is_none() {
                    break;
                }
            }
        }
    }

    #[tokio::test]
    async fn test_append_cases() {
        let mut page_blob = MyPageBlobMock::new();

        let mut append_cache = PageBlobAppendCache::new(8, 8, 10, true);

        append_cache.init_for_tests(&mut page_blob).await;

        let mut payloads = Vec::new();

        payloads.push(vec![5, 5, 5]);

        append_cache
            .append_and_write(&mut page_blob, &payloads)
            .await
            .unwrap();

        let result_buffer = page_blob.download().await.unwrap();

        assert_bytes(
            &result_buffer[0..11],
            &vec![3u8, 0u8, 0u8, 0u8, 5u8, 5u8, 5u8, 0u8, 0u8, 0u8, 0u8],
        );

        let mut payloads = Vec::new();

        payloads.push(vec![6, 6, 6]);

        append_cache
            .append_and_write(&mut page_blob, &payloads)
            .await
            .unwrap();

        let result_buffer = page_blob.download().await.unwrap();

        println!("{:?}", &result_buffer[0..18]);

        assert_bytes(
            &result_buffer[0..18],
            &vec![
                3u8, 0u8, 0u8, 0u8, 5u8, 5u8, 5u8, //First message
                3u8, 0u8, 0u8, 0u8, 6u8, 6u8, 6u8, //Second Message
                0u8, 0u8, 0u8, 0u8, // The end
            ],
        );
    }

    #[tokio::test]
    async fn test_init_pages_on_brand_new_page() {
        let first_payload = compile_payload(5, 513);
        let second_payload = compile_payload(6, 513);

        test_with_two_payloads_script(first_payload.as_slice(), second_payload.as_slice(), 8).await;
    }

    #[tokio::test]
    async fn test_init_pages_than_last_page_fits_512() {
        let first_payload = compile_payload(5, 512 - 4);
        let second_payload = compile_payload(6, 512 - 4);
        test_with_two_payloads_script(first_payload.as_slice(), second_payload.as_slice(), 8).await;
    }

    #[tokio::test]
    async fn test_page_is_beond_of_auto_resize_buffer() {
        let first_payload = compile_payload(5, 1024);
        let second_payload = compile_payload(6, 1024);
        test_with_two_payloads_script(first_payload.as_slice(), second_payload.as_slice(), 2).await;
    }

    async fn test_with_two_payloads_script(
        first_payload: &[u8],
        second_payload: &[u8],
        blob_auto_resize_in_pages: usize,
    ) {
        let mut page_blob = MyPageBlobMock::new();

        let mut append_cache = PageBlobAppendCache::new(8, blob_auto_resize_in_pages, 1024, true);

        append_cache.init_for_tests(&mut page_blob).await;

        let mut payloads = Vec::new();

        payloads.push(first_payload.to_vec());

        append_cache
            .append_and_write(&mut page_blob, &payloads)
            .await
            .unwrap();

        // Load from Blob and Append

        let mut append_cache = PageBlobAppendCache::new(8, blob_auto_resize_in_pages, 1024, true);

        append_cache.init_for_tests(&mut page_blob).await;

        let mut payloads = Vec::new();

        payloads.push(second_payload.to_vec());

        append_cache
            .append_and_write(&mut page_blob, &payloads)
            .await
            .unwrap();

        let buffer_from_blob = page_blob.download().await.unwrap();

        let mut result: Vec<u8> = Vec::new();

        let first_len = (first_payload.len() as i32).to_le_bytes();
        let second_len = (second_payload.len() as i32).to_le_bytes();

        result.extend(&first_len);
        result.extend(first_payload);
        result.extend(&second_len);
        result.extend(second_payload);
        result.extend(&[0u8, 0u8, 0u8, 0u8]);

        assert_bytes(
            result.as_slice(),
            &buffer_from_blob[..first_payload.len() + second_payload.len() + 12],
        );
    }
}
