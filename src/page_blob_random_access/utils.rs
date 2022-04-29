use my_azure_storage_sdk::page_blob::consts::BLOB_PAGE_SIZE;

pub fn get_page_content(src: &[u8], page_id: usize) -> &[u8] {
    let offset = page_id * BLOB_PAGE_SIZE;
    &src[offset..offset + BLOB_PAGE_SIZE]
}


