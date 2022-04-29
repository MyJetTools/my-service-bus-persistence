use my_azure_page_blob::{MyAzurePageBlob, MyPageBlob};

pub async fn resize(page_blob: &MyAzurePageBlob, pages_amount: usize) {
    let mut attempt_no = 0;
    loop {
        match page_blob.resize(pages_amount).await {
            Ok(_) => {
                return;
            }
            Err(err) => {
                super::read_error_handler::is_error_retrieable(
                    page_blob,
                    &err,
                    Some(pages_amount),
                    "save_pages",
                    attempt_no,
                )
                .await;

                attempt_no += 1;
            }
        }
    }
}
