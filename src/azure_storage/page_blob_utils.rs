use my_azure_storage_sdk::{
    page_blob::consts::BLOB_PAGE_SIZE, AzureConnection, AzureStorageError, BlobApi,
    BlobContainersApi, PageBlobApi,
};

pub async fn copy_blob(
    conn_string: &AzureConnection,
    from_container: &str,
    from_blob: &str,
    to_container: &str,
    to_blob: &str,
    buffer_pages_amount: usize,
) -> Result<(), AzureStorageError> {
    let props = conn_string
        .get_blob_properties(from_container, from_blob)
        .await?;

    let mut reamins_pages_to_copy = props.blob_size / BLOB_PAGE_SIZE;

    conn_string
        .create_container_if_not_exist(to_container)
        .await?;

    conn_string
        .create_page_blob_if_not_exists(to_container, to_blob, reamins_pages_to_copy)
        .await?;

    conn_string
        .resize_page_blob(to_container, to_blob, reamins_pages_to_copy)
        .await?;

    let mut page_no = 0;

    while reamins_pages_to_copy > 0 {
        let pages_to_copy = if buffer_pages_amount < reamins_pages_to_copy {
            buffer_pages_amount
        } else {
            reamins_pages_to_copy
        };

        let buffer = conn_string
            .get_pages(from_container, from_blob, page_no, pages_to_copy)
            .await?;

        conn_string
            .save_pages(to_container, to_blob, page_no, buffer)
            .await?;

        reamins_pages_to_copy -= pages_to_copy;
        page_no += pages_to_copy;
    }

    Ok(())
}
