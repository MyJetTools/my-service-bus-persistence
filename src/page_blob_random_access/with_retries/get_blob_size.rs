use my_azure_page_blob::{MyAzurePageBlob, MyPageBlob};

pub async fn get_blob_size(
    page_blob: &MyAzurePageBlob,
    create_if_not_exists_init_size: Option<usize>,
) -> usize {
    let mut attempt_no = 0;
    loop {
        match page_blob.get_blob_properties().await {
            Ok(result) => {
                return result.blob_size;
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
