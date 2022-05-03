use my_azure_storage_sdk::page_blob::AzurePageBlobStorage;

pub async fn resize(page_blob: &AzurePageBlobStorage, pages_amount: usize) {
    let mut attempt_no = 0;
    loop {
        match page_blob.resize(pages_amount).await {
            Ok(_) => {
                return;
            }
            Err(err) => {
                super::read_error_handler::is_error_retrieable(
                    page_blob,
                    err,
                    Some(pages_amount),
                    "save_pages",
                    attempt_no,
                )
                .await
                .unwrap();

                attempt_no += 1;
            }
        }
    }
}
