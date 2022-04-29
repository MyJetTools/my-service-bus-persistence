use my_azure_page_blob::{MyAzurePageBlob, MyPageBlob};

use crate::page_blob_random_access::PageBlobPageId;

pub async fn read_pages(
    page_blob: &MyAzurePageBlob,
    page_no: &PageBlobPageId,
    pages_amount: usize,
    create_if_not_exists_init_size: Option<usize>,
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
                    &err,
                    create_if_not_exists_init_size,
                    "get_blob_size",
                    attempt_no,
                )
                .await;

                attempt_no += 1;
            }
        }
    }
}
