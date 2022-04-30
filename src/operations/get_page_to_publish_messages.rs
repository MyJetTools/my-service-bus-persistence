use std::sync::Arc;

use my_service_bus_shared::page_id::PageId;

use crate::{app::AppContext, message_pages::MessagesPage, topic_data::TopicData};

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

        let storage = app
            .open_or_create_uncompressed_page_storage(topic_data.topic_id.as_str(), &page_id)
            .await;

        let page = MessagesPage::create_uncompressed(page_id, storage).await;

        topic_data.pages_list.add(page_id, Arc::new(page)).await;
    }
}
