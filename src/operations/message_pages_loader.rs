use std::{sync::Arc, time::Duration};

use my_azure_storage_sdk::{AzureConnection, AzureStorageError};

use crate::{
    app::AppContext,
    azure_page_blob_writer::PageBlobAppendCacheError,
    azure_storage::{consts::generage_blob_name, messages_page_blob::MessagesPageBlob},
    compressed_pages::{ClusterPageId, PagesClusterAzureBlob},
    date_time::DateTimeAsMicroseconds,
    message_pages::{MessagePageId, MessagesPage, MessagesPageStorageType},
    messages_protobuf::MessageProtobufModel,
};

pub async fn get_from_compressed_and_uncompressed(
    app: Arc<AppContext>,
    topic_id: &str,
    page_id: MessagePageId,
) -> MessagesPage {
    let load_uncompressed_result = get_uncompressed_page(app.clone(), topic_id, page_id).await;

    if let Some(result) = load_uncompressed_result {
        return result;
    }

    let load_compressed_result = get_compressed_page(app.clone(), topic_id, page_id).await;

    if let Some(result) = load_compressed_result {
        return result;
    }

    return MessagesPage::new_empty(topic_id, page_id.clone(), None, None);
}

async fn get_uncompressed_page(
    app: Arc<AppContext>,
    topic_id: &str,
    page_id: MessagePageId,
) -> Option<MessagesPage> {
    let mut messages_page_blob =
        MessagesPageBlob::new(topic_id.to_string(), page_id.clone(), app.clone());

    let mut attempt_no: usize = 0;

    loop {
        let load_result = messages_page_blob.load().await;

        attempt_no += 1;

        match load_result {
            Ok(messages) => {
                let result = MessagesPage::new(
                    topic_id,
                    page_id.clone(),
                    Some(messages_page_blob),
                    Some(MessagesPageStorageType::UncompressedPage),
                    messages,
                );
                return Some(result);
            }
            Err(err) => {
                app.logs
                    .add_info_string(
                        Some(topic_id),
                        "Reading uncompressed page",
                        format!(
                            "Can not load from uncompressed page. Attempt: #{}, Reason {:?}",
                            attempt_no, err
                        ),
                    )
                    .await;

                if check_if_blob_is_corrupted(&err) {
                    rename_corrupted_file(app.as_ref(), topic_id, page_id)
                        .await
                        .unwrap();
                    return None;
                }

                if let PageBlobAppendCacheError::AzureStorageError(err) = err {
                    if let AzureStorageError::BlobNotFound = err {
                        app.logs
                            .add_info_string(
                                Some(topic_id),
                                "Reading uncompressed page",
                                format!("Blob not found. No Page to load from Uncompressed blob"),
                            )
                            .await;
                        return None;
                    }
                }

                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        }
    }
}

async fn rename_corrupted_file(
    app: &AppContext,
    topic_id: &str,
    page_id: MessagePageId,
) -> Result<(), AzureStorageError> {
    let now = DateTimeAsMicroseconds::now();

    let from_blob_name = generage_blob_name(&page_id);

    let to_blob_name = format!("{}.err-{}", from_blob_name, now.to_rfc3339());

    let connection =
        AzureConnection::from_conn_string(app.settings.messages_connection_string.as_str());

    crate::azure_storage::page_blob_utils::rename_blob_with_retries(
        &connection,
        topic_id,
        from_blob_name.as_str(),
        topic_id,
        to_blob_name.as_str(),
    )
    .await
}

async fn get_compressed_page(
    app: Arc<AppContext>,
    topic_id: &str,
    page_id: MessagePageId,
) -> Option<MessagesPage> {
    let connection =
        AzureConnection::from_conn_string(app.settings.messages_connection_string.as_str());
    let cluster_page_id = ClusterPageId::from_page_id(&page_id);
    let mut compressed_page_loader =
        PagesClusterAzureBlob::new(connection, topic_id, cluster_page_id, app.logs.clone());

    let result = compressed_page_loader.read(&page_id).await;

    let messages: Vec<MessageProtobufModel> = match result {
        Ok(ok) => match ok {
            Some(msg_page) => msg_page,
            None => Vec::new(),
        },
        Err(err) => {
            app.logs
                .add_info_string(Some(topic_id), "Decompressing page", format!("{:?}", err))
                .await;
            Vec::new()
        }
    };

    let result = MessagesPage::new(
        topic_id,
        page_id.clone(),
        None,
        Some(MessagesPageStorageType::CompressedPage),
        messages,
    );

    Some(result)
}

fn check_if_blob_is_corrupted(err: &PageBlobAppendCacheError) -> bool {
    match err {
        PageBlobAppendCacheError::NotInitialized => return false,
        PageBlobAppendCacheError::MaxSizeProtection {
            limit: _,
            size_from_blob: _,
        } => return true,
        PageBlobAppendCacheError::AzureStorageError(azure_error) => match azure_error {
            AzureStorageError::ContainerNotFound => return false,
            AzureStorageError::BlobNotFound => return false,
            AzureStorageError::BlobAlreadyExists => return false,
            AzureStorageError::ContainerBeingDeleted => return false,
            AzureStorageError::ContainerAlreadyExists => return false,
            AzureStorageError::InvalidPageRange => return true,
            AzureStorageError::RequestBodyTooLarge => return false,
            AzureStorageError::UnknownError { msg: _ } => return true,
            AzureStorageError::HyperError { err: _ } => return false,
        },
    }
}
