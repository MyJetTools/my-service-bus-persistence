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

        crate::operations::restore_page::open_or_create(app, topic_data, page_id).await;
    }
}
