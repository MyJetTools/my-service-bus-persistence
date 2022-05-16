use std::sync::Arc;

use my_service_bus_shared::page_id::PageId;

use crate::{app::AppContext, topic_data::TopicData, uncompressed_page::UncompressedPage};

pub async fn get_page_to_publish_messages(
    app: &AppContext,
    topic_data: &TopicData,
    page_id: PageId,
) -> Arc<UncompressedPage> {
    loop {
        let page = topic_data.uncompressed_pages_list.get(page_id).await;

        if let Some(page) = page {
            return page;
        };

        crate::operations::restore_page::open_or_create(app, topic_data, page_id).await;
    }
}
