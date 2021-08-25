use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::{
    app::AppContext,
    azure_storage::messages_page_blob::MessagesPageBlob,
    message_pages::{MessagePageId, MessagesPage},
    toipics_snapshot::TopicsDataProtobufModel,
    utils::StopWatch,
};

use my_azure_page_blob::*;
use my_azure_storage_sdk::AzureStorageError;

pub async fn execute(app: &AppContext, topics: &TopicsDataProtobufModel) {
    let active_pages = get_active_pages(&topics);

    let data_by_topic = app.data_by_topic.write().await;

    for (topic_id, active_pages) in active_pages {
        let topic_data = data_by_topic.get(topic_id.as_str());

        if topic_data.is_none() {
            continue;
        }

        let topic_data = topic_data.unwrap();

        let gc_pages = topic_data
            .as_ref()
            .gc_if_needed(active_pages.as_slice())
            .await;

        if let Some(gc_pages) = gc_pages {
            compress_pages_if_needed_after_gc(app, gc_pages.as_slice()).await;
        }
    }
}

pub fn get_active_pages(s: &TopicsDataProtobufModel) -> HashMap<String, Vec<MessagePageId>> {
    let mut result = HashMap::new();

    for topic in &s.data {
        let mut pages = Vec::new();

        let page = MessagePageId::from_message_id(topic.message_id);

        pages.push(page);

        for queue in &topic.queues {
            for range in &queue.ranges {
                let from_id = MessagePageId::from_message_id(range.from_id);

                let to_id = MessagePageId::from_message_id(range.from_id);

                if pages.iter().all(|itm| itm.value != from_id.value) {
                    pages.push(from_id);
                }

                if pages.iter().all(|itm| itm.value != to_id.value) {
                    pages.push(to_id);
                }
            }
        }

        result.insert(topic.topic_id.to_string(), pages);
    }

    result
}

const PAGE_COMPRESSION_LOG_PROCESS: &str = "Page Compression";

pub async fn compress_pages_if_needed_after_gc(app: &AppContext, pages: &[Arc<MessagesPage>]) {
    for page in pages {
        let page = page.as_ref();

        if page.messages_amount().await == 0 {
            app.logs
                .add_info(
                    Some(page.topic_id.as_str()),
                    PAGE_COMPRESSION_LOG_PROCESS,
                    "Page has no messages.. Skipping",
                )
                .await;
            continue;
        }

        if page.has_compressed_copy().await {
            app.logs
                .add_info(
                    Some(page.topic_id.as_str()),
                    PAGE_COMPRESSION_LOG_PROCESS,
                    "Page is already compressed.. Skipping",
                )
                .await;
            continue;
        }

        let mut sw = StopWatch::new();

        sw.start();

        let compressed = compress_page(page, app).await;

        if compressed {
            let mut storage = page.storage.lock().await;

            if let Some(messages_page_blob) = &mut storage.blob {
                delete_uncompressed_page(messages_page_blob, app).await;
            }
        }

        sw.pause();

        app.logs
            .add_info_string(
                Some(page.topic_id.as_str()),
                PAGE_COMPRESSION_LOG_PROCESS,
                format!(
                    "Page #{} compression completed in {:?}",
                    page.page_id.value,
                    sw.duration()
                ),
            )
            .await
    }
}

async fn compress_page(page: &MessagesPage, app: &AppContext) -> bool {
    let mut attempt_no: usize = 0;

    loop {
        let result = app
            .compressed_page_blob
            .compress_page(page, app.logs.clone())
            .await;

        match result {
            Ok(()) => {
                app.logs
                    .as_ref()
                    .add_info_string(
                        Some(page.topic_id.as_str()),
                        PAGE_COMPRESSION_LOG_PROCESS,
                        format!("Page #{} is compressed", page.page_id.value),
                    )
                    .await;

                return true;
            }
            Err(err) => {
                app.logs
                    .as_ref()
                    .add_error_str(
                        Some(page.topic_id.as_str()),
                        PAGE_COMPRESSION_LOG_PROCESS,
                        format!(
                            "Cannot compress page #{}.  Attempt: #{}",
                            page.page_id.value, attempt_no
                        ),
                        format!("{:?}", err),
                    )
                    .await;

                attempt_no += 1;

                if attempt_no >= 5 {
                    app.logs
                        .as_ref()
                        .add_error_str(
                            Some(page.topic_id.as_str()),
                            PAGE_COMPRESSION_LOG_PROCESS,
                            format!(
                                "Can not compress page #{}.  Attempt: #{}",
                                page.page_id.value, attempt_no
                            ),
                            format!("Attempts amount reached maximum. Skipping operation"),
                        )
                        .await;
                    return false;
                }

                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

async fn delete_uncompressed_page(messages_page_blob: &mut MessagesPageBlob, app: &AppContext) {
    let mut attempt_no: usize = 0;

    loop {
        let result = messages_page_blob.blob.delete().await;

        match result {
            Ok(()) => {
                app.logs
                    .as_ref()
                    .add_info_string(
                        Some(messages_page_blob.topic_id.as_str()),
                        PAGE_COMPRESSION_LOG_PROCESS,
                        format!(
                            "Delete uncompressed page #{}",
                            messages_page_blob.page_id.value
                        ),
                    )
                    .await;
                return;
            }
            Err(err) => {
                let handled =
                    handle_delete_uncompressed_data_error(messages_page_blob, app, err).await;

                if handled {
                    return;
                }

                attempt_no += 1;

                if attempt_no >= 5 {
                    app.logs
                        .as_ref()
                        .add_error_str(
                            Some(messages_page_blob.topic_id.as_str()),
                            PAGE_COMPRESSION_LOG_PROCESS,
                            format!(
                                "Can not delete uncompressed page #{}.  Attempt: #{}",
                                messages_page_blob.page_id.value, attempt_no
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
        messages_page_blob: &mut MessagesPageBlob,
        app: &AppContext,
        err: AzureStorageError,
    ) -> bool {
        app.logs
            .as_ref()
            .add_error_str(
                Some(messages_page_blob.topic_id.as_str()),
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
