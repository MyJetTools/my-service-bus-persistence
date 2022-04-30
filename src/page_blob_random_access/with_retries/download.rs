use my_azure_storage_sdk::page_blob::AzurePageBlobStorage;

pub async fn download(
    page_blob: &AzurePageBlobStorage,
    create_if_not_exists_init_size: Option<usize>,
) -> Vec<u8> {
    let mut attempt_no = 0;
    loop {
        match page_blob.download().await {
            Ok(result) => {
                return result;
            }
            Err(err) => {
                super::read_error_handler::is_error_retrieable(
                    page_blob,
                    err,
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
