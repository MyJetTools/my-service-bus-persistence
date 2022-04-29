use my_azure_storage_sdk::page_blob::consts::BLOB_PAGE_SIZE;

#[derive(Debug, Clone, Copy)]
pub struct PageBlobPageId {
    pub value: usize,
}

impl PageBlobPageId {
    pub fn new(value: usize) -> Self {
        Self { value }
    }

    pub fn from_blob_position(blob_position: usize) -> Self {
        Self {
            value: blob_position / BLOB_PAGE_SIZE,
        }
    }

    pub fn get_absolute_position(&self) -> usize {
        self.value * BLOB_PAGE_SIZE
    }
}

pub fn get_page_content(src: &[u8], page_id: usize) -> &[u8] {
    let offset = page_id * BLOB_PAGE_SIZE;
    &src[offset..offset + BLOB_PAGE_SIZE]
}
