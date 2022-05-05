use my_azure_storage_sdk::{
    page_blob::{consts::BLOB_PAGE_SIZE, AzurePageBlobStorage},
    AzureStorageError,
};

use crate::page_blob_random_access::PageBlobPageId;

pub async fn save_pages(
    page_blob: &AzurePageBlobStorage,
    page_no: &PageBlobPageId,
    content: &[u8],
    max_pages_amount_chunk: usize,
) {
    let mut attempt_no = 0;
    loop {
        let pages_amount = content.len() / BLOB_PAGE_SIZE;

        let result = if pages_amount > max_pages_amount_chunk {
            save_by_chunks(
                page_blob,
                page_no,
                content,
                pages_amount,
                max_pages_amount_chunk,
            )
            .await
        } else {
            page_blob.save_pages(page_no.value, content.to_vec()).await
        };

        match result {
            Ok(_) => {
                return;
            }
            Err(err) => {
                super::write_error_handler::is_error_retrieable(err, "save_pages", attempt_no)
                    .await
                    .unwrap();

                attempt_no += 1;
            }
        }
    }
}

//TODO - UnitTest it
async fn save_by_chunks(
    page_blob: &AzurePageBlobStorage,
    page_no: &PageBlobPageId,
    content: &[u8],
    content_pages_amount: usize,
    max_pages_chunk: usize,
) -> Result<(), AzureStorageError> {
    let mut content_pages_amount = content_pages_amount;
    let mut page_no = page_no.value;

    while content_pages_amount > max_pages_chunk {
        let start_pos = page_no * BLOB_PAGE_SIZE;
        let end_pos = start_pos + max_pages_chunk * BLOB_PAGE_SIZE;

        let payload_to_save = &content[start_pos..end_pos];
        page_blob
            .save_pages(page_no, payload_to_save.to_vec())
            .await?;

        page_no += max_pages_chunk;
        content_pages_amount -= max_pages_chunk;
    }

    if content_pages_amount > 0 {
        let start_pos = page_no * BLOB_PAGE_SIZE;
        let end_pos = start_pos + max_pages_chunk * BLOB_PAGE_SIZE;
        let payload_to_save = &content[start_pos..end_pos];
        page_blob
            .save_pages(page_no, payload_to_save.to_vec())
            .await?;
    }

    Ok(())
}
