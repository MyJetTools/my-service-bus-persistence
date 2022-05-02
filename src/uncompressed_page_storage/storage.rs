use crate::page_blob_random_access::{PageBlobPageId, PageBlobRandomAccess};

use super::toc::{UncompressedFileToc, TOC_SIZE, TOC_SIZE_IN_PAGES};

pub async fn read_toc(page_blob: &mut PageBlobRandomAccess) -> UncompressedFileToc {
    let size = page_blob.get_blob_size(Some(TOC_SIZE_IN_PAGES)).await;

    if size < TOC_SIZE {
        page_blob.resize_blob(TOC_SIZE_IN_PAGES).await;
    }

    let content = page_blob
        .load_pages(
            &PageBlobPageId::new(0),
            TOC_SIZE_IN_PAGES,
            Some(TOC_SIZE_IN_PAGES),
        )
        .await;

    UncompressedFileToc::new(content)
}
