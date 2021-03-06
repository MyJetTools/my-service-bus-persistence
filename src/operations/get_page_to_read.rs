use std::sync::Arc;

use crate::{
    app::AppContext,
    message_pages::{MessagePageId, MessagesPage},
    topic_data::TopicData,
};

pub async fn get_page_to_read(
    app: &AppContext,
    topic_data: &TopicData,
    page_id: &MessagePageId,
) -> Arc<MessagesPage> {
    loop {
        let page = topic_data.pages_list.get(page_id.value).await;

        if let Some(page) = page {
            return page;
        };

        crate::operations::restore_page::open_uncompressed_or_empty(app, topic_data, page_id.value)
            .await;
    }
}
