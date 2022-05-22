use std::sync::Arc;

use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::{app::AppContext, topic_data::TopicData, uncompressed_page::UncompressedPageId};

use super::OperationError;

pub async fn gc_pages(
    app: &AppContext,
    topic_data: Arc<TopicData>,
    active_pages: &[UncompressedPageId],
) -> Result<(), OperationError> {
    let mut pages_to_gc = get_pages_to_gc(topic_data.as_ref(), active_pages).await;

    if pages_to_gc.len() == 0 {
        return Ok(());
    }

    for page_to_gc in pages_to_gc.drain(..) {
        if let Some(_) = topic_data
            .uncompressed_pages_list
            .remove_page(page_to_gc)
            .await
        {
            app.logs.add_info(
                Some(topic_data.topic_id.as_str()),
                "Page GC",
                format!("Page {} is garbage collected", page_to_gc),
            );
        }
    }

    Ok(())
}

async fn get_pages_to_gc(topic: &TopicData, active_pages: &[UncompressedPageId]) -> Vec<i64> {
    let now = DateTimeAsMicroseconds::now();

    let pages = topic.uncompressed_pages_list.get_all().await;

    let mut pages_to_gc = Vec::new();

    for page in pages {
        if active_pages
            .iter()
            .all(|itm| itm.value != page.page_id.value)
        {
            if now.seconds_before(page.as_ref().metrics.get_last_access()) > 30 {
                pages_to_gc.push(page.page_id.value);
            }
        }
    }
    pages_to_gc
}
