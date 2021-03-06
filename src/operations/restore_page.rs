use std::sync::Arc;

use my_service_bus_shared::page_id::PageId;

use crate::{app::AppContext, message_pages::MessagesPage, topic_data::TopicData};

pub async fn open_or_create(app: &AppContext, topic_data: &TopicData, page_id: PageId) {
    let storage = app
        .open_or_create_uncompressed_page_storage(topic_data.topic_id.as_str(), &page_id)
        .await;

    let page =
        MessagesPage::create_uncompressed(page_id, storage, app.get_max_payload_size()).await;

    topic_data.pages_list.add(page_id, Arc::new(page)).await;
}

pub async fn open_uncompressed_or_empty(app: &AppContext, topic_data: &TopicData, page_id: PageId) {
    let page = app
        .open_uncompressed_page_storage_if_exists(topic_data.topic_id.as_str(), &page_id)
        .await;

    let page = if let Some(storage) = page {
        MessagesPage::create_uncompressed(page_id, storage, app.get_max_message_size()).await
    } else {
        MessagesPage::create_as_empty(page_id)
    };

    topic_data.pages_list.add(page_id, Arc::new(page)).await;
}
