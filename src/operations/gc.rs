use std::sync::Arc;

use my_service_bus_shared::date_time::DateTimeAsMicroseconds;

use crate::{
    app::{AppContext, TopicData},
    message_pages::MessagePageId,
};

use super::OperationError;

pub async fn gc_if_needed(
    app: &AppContext,
    topic_data: Arc<TopicData>,
    active_pages: &[MessagePageId],
) -> Result<(), OperationError> {
    let mut pages_to_gc = get_pages_to_gc(topic_data.as_ref(), active_pages).await;

    if pages_to_gc.len() == 0 {
        return Ok(());
    }

    for page_to_gc in pages_to_gc.drain(..) {
        let removed_page = topic_data.remove_page(page_to_gc).await;
        if let Some(page) = removed_page {
            let mut write_access = page.data.lock().await;
            super::page_compression::execute(topic_data.clone(), &mut *write_access, app).await?;
        }

        app.logs
            .add_info(
                Some(topic_data.topic_id.as_str()),
                "Page GC",
                format!("Page {} is garbage collected", page_to_gc),
            )
            .await;
    }

    Ok(())
}

async fn get_pages_to_gc(topic: &TopicData, active_pages: &[MessagePageId]) -> Vec<i64> {
    let now = DateTimeAsMicroseconds::now();

    let pages = topic.get_all().await;

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
