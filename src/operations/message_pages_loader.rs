use std::{collections::BTreeMap, sync::Arc, time::Duration};

use my_azure_page_blob_append::PageBlobAppendCacheError;
use my_azure_storage_sdk::{AzureConnection, AzureStorageError};
use my_service_bus_shared::{
    date_time::DateTimeAsMicroseconds, protobuf_models::MessageProtobufModel, MessageId,
};

use crate::{
    app::{AppContext, TopicData},
    azure_storage::{consts::generage_blob_name, messages_page_blob::MessagesPageBlob},
    message_pages::{MessagePageId, MessagesPageData},
    uncompressed_pages::UncompressedPage,
};

pub async fn load_page(
    app: Arc<AppContext>,
    topic_data: Arc<TopicData>,
    page_id: MessagePageId,
    page_id_current: bool,
) -> MessagesPageData {
    let result = load_uncompressed_page(app.clone(), topic_data.topic_id.as_str(), page_id).await;

    if let Some(result) = result {
        return result;
    }

    let result = load_compressed_page(app.as_ref(), topic_data.as_ref(), page_id).await;

    if let Some(result) = result {
        return result;
    }

    if page_id_current {
        return init_current_blob(app.clone(), topic_data.topic_id.as_str(), page_id).await;
    }

    return MessagesPageData::new_blank();
}

async fn load_uncompressed_page(
    app: Arc<AppContext>,
    topic_id: &str,
    page_id: MessagePageId,
) -> Option<MessagesPageData> {
    let mut messages_page_blob =
        MessagesPageBlob::new(topic_id.to_string(), page_id.clone(), app.clone());

    let mut attempt_no: usize = 0;

    loop {
        let load_result = messages_page_blob.load().await;

        attempt_no += 1;

        match load_result {
            Ok(messages) => {
                let as_tree_map = to_tree_map(messages);

                let result = MessagesPageData::restored_uncompressed(
                    page_id.clone(),
                    as_tree_map,
                    messages_page_blob,
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

async fn load_compressed_page(
    app: &AppContext,
    topic_data: &TopicData,
    page_id: MessagePageId,
) -> Option<MessagesPageData> {
    let read_result = topic_data.pages_cluster.read(page_id).await;

    match read_result {
        Ok(payload) => {
            if let Some(zip_archive) = payload {
                return MessagesPageData::resored_compressed(page_id, zip_archive);
            }

            return None;
        }
        Err(err) => {
            app.logs
                .add_error(
                    Some(topic_data.topic_id.as_str()),
                    "Load compressed page",
                    "Can not restore from compressed page",
                    format!("{:?}", err),
                )
                .await;

            return None;
        }
    }
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

async fn init_current_blob(
    app: Arc<AppContext>,
    topic_id: &str,
    page_id: MessagePageId,
) -> MessagesPageData {
    let mut blob = MessagesPageBlob::new(topic_id.to_string(), page_id, app);
    blob.create_new().await;
    let uncompressed_page = UncompressedPage::new_empty(page_id, blob);

    MessagesPageData::Uncompressed(uncompressed_page)
}

fn to_tree_map(msgs: Vec<MessageProtobufModel>) -> BTreeMap<MessageId, MessageProtobufModel> {
    let mut result = BTreeMap::new();

    for msg in msgs {
        result.insert(msg.message_id, msg);
    }

    result
}
