use std::sync::Arc;

use crate::{app::AppContext, topic_data::TopicData, uncompressed_page::*};

pub async fn get_uncompressed_page_to_read(
    app: &AppContext,
    topic_data: &TopicData,
    page_id: &UncompressedPageId,
) -> Option<Arc<UncompressedPage>> {
    let page = topic_data.uncompressed_pages_list.get(page_id.value).await;

    if let Some(page) = page {
        return Some(page);
    };

    crate::operations::restore_page::open_uncompressed_if_exists(app, topic_data, page_id.value)
        .await
}
