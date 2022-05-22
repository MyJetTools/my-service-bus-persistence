use std::sync::Arc;

use crate::{
    app::AppContext,
    topic_data::TopicData,
    uncompressed_page::{UncompressedPage, UncompressedPageId},
};

pub async fn open_or_create(
    app: &AppContext,
    topic_data: &TopicData,
    page_id: &UncompressedPageId,
) {
    let storage = app
        .open_or_create_uncompressed_page_storage(topic_data.topic_id.as_str(), page_id)
        .await;

    let page = UncompressedPage::new(page_id.clone(), storage, app.get_max_payload_size()).await;

    topic_data
        .uncompressed_pages_list
        .add(page_id.value, Arc::new(page))
        .await;
}

pub async fn open_if_exists(
    app: &AppContext,
    topic_data: &TopicData,
    page_id: &UncompressedPageId,
) -> Option<Arc<UncompressedPage>> {
    let storage = app
        .open_uncompressed_page_storage_if_exists(topic_data.topic_id.as_str(), page_id)
        .await?;

    let page = UncompressedPage::new(page_id.clone(), storage, app.get_max_payload_size()).await;

    let page = Arc::new(page);

    topic_data
        .uncompressed_pages_list
        .add(page_id.value, page.clone())
        .await;

    Some(page)
}
