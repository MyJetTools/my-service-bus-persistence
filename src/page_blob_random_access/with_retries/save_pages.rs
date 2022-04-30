use my_azure_storage_sdk::page_blob::AzurePageBlobStorage;

use crate::page_blob_random_access::PageBlobPageId;

pub async fn save_pages(
    page_blob: &AzurePageBlobStorage,
    page_no: &PageBlobPageId,
    content: &[u8],
) {
    let mut attempt_no = 0;
    loop {
        match page_blob.save_pages(page_no.value, content.to_vec()).await {
            Ok(_) => {
                return;
            }
            Err(err) => {
                super::write_error_handler::is_error_retrieable(
                    page_blob,
                    &err,
                    "save_pages",
                    attempt_no,
                )
                .await;

                attempt_no += 1;
            }
        }
    }
}
