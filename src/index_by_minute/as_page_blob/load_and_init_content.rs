use my_azure_storage_sdk::page_blob::consts::BLOB_PAGE_SIZE;

use crate::{
    index_by_minute::utils::MINUTE_INDEX_FILE_SIZE, page_blob_random_access::PageBlobRandomAccess,
};

pub static init_pages_size: usize = MINUTE_INDEX_FILE_SIZE / BLOB_PAGE_SIZE;

pub async fn load_and_init_content(page_blob: &mut PageBlobRandomAccess) -> Vec<u8> {
    let file_size = page_blob.get_blob_size(Some(init_pages_size)).await;

    if file_size < MINUTE_INDEX_FILE_SIZE {
        page_blob.resize_blob(init_pages_size);
        let empty_content = [0u8; MINUTE_INDEX_FILE_SIZE];
        return empty_content.to_vec();
    }

    page_blob.download(Some(init_pages_size)).await
}
