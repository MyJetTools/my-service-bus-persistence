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

pub async fn delete_blob_with_retries(
    connection: &AzureConnection,
    container_name: &str,
    blob_name: &str,
) -> Result<(), AzureStorageError> {
    loop {
        let result = connection
            .delete_blob_if_exists(container_name, blob_name)
            .await;

        if result.is_ok() {
            break;
        }

        let err = result.err().unwrap();

        match err {
            AzureStorageError::ContainerNotFound => {
                break;
            }
            AzureStorageError::BlobNotFound => {
                break;
            }
            AzureStorageError::ContainerBeingDeleted => {
                break;
            }
            _ => {
                return Err(err);
            }
        }
    }

    Ok(())
}

pub async fn rename_blob_with_retries(
    connection: &AzureConnection,
    from_container: &str,
    from_blob: &str,
    to_container: &str,
    to_blob: &str,
) -> Result<(), AzureStorageError> {
    loop {
        let result = crate::azure_storage::page_blob_utils::copy_blob(
            &connection,
            from_container,
            from_blob,
            to_container,
            to_blob,
            5_000,
        )
        .await;

        if result.is_ok() {
            break;
        }
    }

    delete_blob_with_retries(connection, from_container, from_blob).await?;

    Ok(())
}
