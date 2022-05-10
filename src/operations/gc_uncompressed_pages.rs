use std::sync::Arc;

use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::{
    app::AppContext,
    message_pages::{CompressedPageId, MessagePageId, MessagesPage, MessagesPageType},
    topic_data::TopicData,
};

use super::OperationError;

pub async fn gc_uncompressed_pages(
    app: &AppContext,
    topic_data: Arc<TopicData>,
    active_pages: &[MessagePageId],
) -> Result<(), OperationError> {
    let mut pages_to_gc = get_pages_to_gc(topic_data.as_ref(), active_pages).await;

    if pages_to_gc.len() == 0 {
        return Ok(());
    }

    for page_to_gc in pages_to_gc.drain(..) {
        if let Some(page) = topic_data.pages_list.remove_page(page_to_gc).await {
            compress_pages(app, topic_data.as_ref(), page.as_ref()).await;
            app.delete_uncompressed_page_blob(topic_data.topic_id.as_str(), page_to_gc)
                .await;
            app.logs.add_info(
                Some(topic_data.topic_id.as_str()),
                "Page GC",
                format!("Page {} is garbage collected", page_to_gc),
            );
        }
    }

    Ok(())
}

async fn get_pages_to_gc(topic_data: &TopicData, active_pages: &[MessagePageId]) -> Vec<i64> {
    let now = DateTimeAsMicroseconds::now();

    let pages = topic_data.pages_list.get_all().await;

    let mut pages_to_gc = Vec::new();

    for page in pages {
        if active_pages
            .iter()
            .all(|itm| itm.value != page.get_page_id())
        {
            if now.seconds_before(page.as_ref().metrics.get_last_access()) > 30 {
                pages_to_gc.push(page.get_page_id());
            }
        }
    }
    pages_to_gc
}

async fn compress_pages(app: &AppContext, topic_data: &TopicData, page: &MessagesPage) {
    if let MessagesPageType::Uncompressed(uncompressed_page) = &page.page_type {
        let first_compressed_page_id =
            CompressedPageId::from_message_id(uncompressed_page.page_id.get_first_message_id());

        for compressed_page_id in
            first_compressed_page_id.value..first_compressed_page_id.value + 100
        {
            let compressed_page_id = CompressedPageId::new(compressed_page_id);

            crate::operations::compress_page_if_needed(
                app,
                topic_data,
                &compressed_page_id,
                uncompressed_page,
            )
            .await;
        }
    }
}
