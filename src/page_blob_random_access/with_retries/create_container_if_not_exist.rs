use my_azure_storage_sdk::page_blob::AzurePageBlobStorage;

pub async fn create_container_if_not_exist(page_blob: &AzurePageBlobStorage) {
    let mut attempt_no = 0;
    loop {
        match page_blob.create_container_if_not_exist().await {
            Ok(_) => {
                return;
            }
            Err(err) => {
                super::write_error_handler::is_error_retrieable(
                    err,
                    "create_container_if_not_exist",
                    attempt_no,
                )
                .await
                .unwrap();

                attempt_no += 1;
            }
        }
    }
}
