use my_azure_storage_sdk::page_blob::AzurePageBlobStorage;

use crate::page_blob_random_access::PageBlobPageId;

pub async fn read_pages(
    page_blob: &AzurePageBlobStorage,
    page_no: &PageBlobPageId,
    pages_amount: usize,
    create_if_not_exists_init_pages_amount: Option<usize>,
) -> Vec<u8> {
    let mut attempt_no = 0;
    loop {
        match page_blob.get(page_no.value, pages_amount).await {
            Ok(result) => {
                return result;
            }
            Err(err) => {
                super::read_error_handler::is_error_retrieable(
                    page_blob,
                    err,
                    create_if_not_exists_init_pages_amount,
                    "get_blob_size",
                    attempt_no,
                )
                .await
                .unwrap();

                attempt_no += 1;
            }
        }
    }
}
