use my_azure_storage_sdk::page_blob::AzurePageBlobStorage;

pub async fn get_blob_properties(page_blob: &AzurePageBlobStorage) -> Option<usize> {
    let mut attempt_no = 0;
    loop {
        match page_blob.get_blob_properties().await {
            Ok(result) => {
                return Some(result.blob_size);
            }
            Err(err) => {
                if super::read_error_handler::is_error_retrieable(
                    page_blob,
                    err,
                    None,
                    "get_blob_size",
                    attempt_no,
                )
                .await
                .is_ok()
                {
                    attempt_no += 1;
                } else {
                    return None;
                }
            }
        }
    }
}
