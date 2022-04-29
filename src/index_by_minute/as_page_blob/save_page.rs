use crate::page_blob_random_access::{PageBlobPageId, PageBlobRandomAccess};

pub async fn save_page(
    page_blob: &mut PageBlobRandomAccess,
    page_no: &PageBlobPageId,
    content: &[u8],
) {
    page_blob.save_pages(page_no, content).await;
}
