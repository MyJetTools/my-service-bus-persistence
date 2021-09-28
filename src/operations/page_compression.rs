use std::{sync::Arc, time::Duration};

use my_azure_page_blob::MyPageBlob;
use my_azure_storage_sdk::AzureStorageError;
use my_service_bus_shared::page_compressor::CompressedPageBuilder;

use crate::{
    app::{AppContext, TopicData},
    message_pages::MessagesPageData,
    uncompressed_pages::UncompressedPage,
};

use super::OperationError;

const PAGE_COMPRESSION_LOG_PROCESS: &str = "Page Compression";

pub async fn execute(
    topic_data: Arc<TopicData>,
    page: &mut MessagesPageData,
    app: &AppContext,
) -> Result<Option<Vec<u8>>, OperationError> {
    if let MessagesPageData::Uncompressed(uncompressed_page) = page {
        let mut page_builder = CompressedPageBuilder::new();

        for msg in uncompressed_page.messages.values() {
            let mut payload = Vec::new();
            msg.serialize(&mut payload)?;

            page_builder.add_message(msg.message_id, payload.as_slice())?;
        }

        let compressed = page_builder.get_payload()?;

        topic_data
            .pages_cluster
            .write(uncompressed_page.page_id, compressed.as_slice())
            .await;

        delete_uncompressed_page(topic_data.topic_id.as_str(), uncompressed_page, app).await;

        return Ok(Some(compressed));
    }

    return Ok(None);
}

async fn delete_uncompressed_page(topic_id: &str, page: &mut UncompressedPage, app: &AppContext) {
    let mut attempt_no: usize = 0;

    loop {
        match page.blob.blob.delete().await {
            Ok(()) => {
                app.logs
                    .as_ref()
                    .add_info_string(
                        Some(topic_id),
                        PAGE_COMPRESSION_LOG_PROCESS,
                        format!("Delete uncompressed page #{}", page.page_id.value),
                    )
                    .await;
                return;
            }
            Err(err) => {
                let handled = handle_delete_uncompressed_data_error(topic_id, page, app, err).await;

                if handled {
                    return;
                }

                attempt_no += 1;

                if attempt_no >= 5 {
                    app.logs
                        .as_ref()
                        .add_error_str(
                            Some(topic_id),
                            PAGE_COMPRESSION_LOG_PROCESS,
                            format!(
                                "Can not delete uncompressed page #{}.  Attempt: #{}",
                                page.page_id.value, attempt_no
                            ),
                            format!("Attempts amount reached maximum. Skipping operation"),
                        )
                        .await;
                    return;
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }

    async fn handle_delete_uncompressed_data_error(
        topic_id: &str,
        messages_page_blob: &mut UncompressedPage,
        app: &AppContext,
        err: AzureStorageError,
    ) -> bool {
        app.logs
            .as_ref()
            .add_error_str(
                Some(topic_id),
                PAGE_COMPRESSION_LOG_PROCESS,
                format!(
                    "Can not delete uncompressed page #{}",
                    messages_page_blob.page_id.value
                ),
                format!("{:?}", err),
            )
            .await;

        match err {
            AzureStorageError::BlobNotFound => {
                return true;
            }
            _ => {
                return false;
            }
        }
    }
}
