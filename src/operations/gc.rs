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
                "Page is garbage collected",
            )
            .await;
    }

    Ok(())
}

async fn get_pages_to_gc(topic: &TopicData, active_pages: &[MessagePageId]) -> Vec<i64> {
    let now = DateTimeAsMicroseconds::now();

    let pages_read_access = topic.pages.lock().await;

    let mut pages_to_gc = Vec::new();

    for (page_id, page) in &*pages_read_access {
        if active_pages.iter().all(|itm| itm.value != *page_id) {
            if now.seconds_before(page.as_ref().get_last_access().await) > 30 {
                pages_to_gc.push(*page_id);
            }
        }
    }
    pages_to_gc
}
