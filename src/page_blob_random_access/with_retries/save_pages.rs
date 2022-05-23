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
            save_by_chunks(page_blob, page_no, content, max_pages_amount_chunk).await
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
    max_pages_chunk: usize,
) -> Result<(), AzureStorageError> {
    let mut content_pages_amount = content.len() / BLOB_PAGE_SIZE;
    let mut page_no = page_no.value;

    println!("Saving by chunks. Content Len: {}", content.len());

    while content_pages_amount > max_pages_chunk {
        let start_pos = page_no * BLOB_PAGE_SIZE;
        let end_pos = start_pos + max_pages_chunk * BLOB_PAGE_SIZE;

        let payload_to_save = &content[start_pos..end_pos];

        println!(
            "Saving by chunks inside the loop. Pages: {}-{}. Content Len: {}-{}",
            page_no, max_pages_chunk, start_pos, end_pos
        );

        page_blob
            .save_pages(page_no, payload_to_save.to_vec())
            .await?;

        page_no += max_pages_chunk;
        content_pages_amount -= max_pages_chunk;
    }

    if content_pages_amount > 0 {
        let start_pos = page_no * BLOB_PAGE_SIZE;
        let end_pos = start_pos + content_pages_amount * BLOB_PAGE_SIZE;

        println!(
            "Saving by chunks last chunk. Pages: {}-{}. Content : {}-{}",
            page_no, content_pages_amount, start_pos, end_pos
        );

        let payload_to_save = &content[start_pos..end_pos];
        page_blob
            .save_pages(page_no, payload_to_save.to_vec())
            .await?;
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use my_azure_storage_sdk::{
        page_blob::{consts::BLOB_PAGE_SIZE, AzurePageBlobStorage},
        AzureStorageConnection,
    };

    use super::*;
    use crate::page_blob_random_access::PageBlobPageId;

    #[tokio::test]
    async fn test_save_by_chunks() {
        let mut payload = Vec::new();

        payload.extend_from_slice(&[1u8; BLOB_PAGE_SIZE].as_ref());
        payload.extend_from_slice(&[2u8; BLOB_PAGE_SIZE].as_ref());
        payload.extend_from_slice(&[3u8; BLOB_PAGE_SIZE].as_ref());
        payload.extend_from_slice(&[4u8; BLOB_PAGE_SIZE].as_ref());
        payload.extend_from_slice(&[5u8; BLOB_PAGE_SIZE].as_ref());

        let conn_string = AzureStorageConnection::new_in_memory();
        let page_blob = AzurePageBlobStorage::new(
            Arc::new(conn_string),
            "test".to_string(),
            "test".to_string(),
        )
        .await;

        page_blob.create_container_if_not_exist().await.unwrap();
        page_blob.create_if_not_exists(5).await.unwrap();

        save_by_chunks(&page_blob, &PageBlobPageId::new(0), &payload, 2)
            .await
            .unwrap();

        let result = page_blob.download().await.unwrap();

        assert_eq!(payload, result)
    }
}
