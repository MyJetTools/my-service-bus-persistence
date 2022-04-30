use std::{collections::HashMap, sync::Arc};

use my_service_bus_shared::page_id::PageId;

use crate::{
    app::AppContext, message_pages::MessagesPage, topic_data::TopicData,
    uncompressed_page_storage::UncompressedPageStorage,
};

pub async fn get_page_to_publish_messages(
    app: &AppContext,
    topic_data: &TopicData,
    page_id: PageId,
) -> Arc<MessagesPage> {
    loop {
        let page = topic_data.pages_list.get(page_id).await;

        if let Some(page) = page {
            if page.is_uncompressed() {
                return page;
            }
        };

        let mut storages = topic_data.storages.lock().await;

        let page = create_uncompressed(app, topic_data, page_id, &mut storages).await;
        topic_data.pages_list.add(page_id, Arc::new(page)).await;
    }
}

async fn create_uncompressed(
    app: &AppContext,
    topic_data: &TopicData,
    page_id: PageId,
    uncomopressed_storages: &mut HashMap<PageId, UncompressedPageStorage>,
) -> MessagesPage {
    let mut storage = app
        .open_or_create_uncompressed_page_storage(topic_data.topic_id.as_str(), &page_id)
        .await;

    let toc = storage.read_toc().await;

    let messages_page = MessagesPage::create_uncompressed(page_id, toc);

    uncomopressed_storages.insert(page_id, storage);

    messages_page
}
