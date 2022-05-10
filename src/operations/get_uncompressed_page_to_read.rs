use std::sync::Arc;

use crate::{
    app::AppContext,
    message_pages::{MessagePageId, MessagesPage},
    topic_data::TopicData,
};

pub async fn get_uncompressed_page_to_read(
    app: &AppContext,
    topic_data: &TopicData,
    page_id: &MessagePageId,
) -> Option<Arc<MessagesPage>> {
    let page = topic_data.pages_list.get(page_id.value).await;

    if let Some(page) = page {
        return Some(page);
    };

    crate::operations::restore_page::open_uncompressed_if_exists(app, topic_data, page_id.value)
        .await
}
