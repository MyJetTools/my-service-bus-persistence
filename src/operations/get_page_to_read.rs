use std::sync::Arc;

use crate::{
    app::AppContext,
    message_pages::{MessagePageId, MessagesPage},
    topic_data::TopicData,
};

use super::OperationError;

pub async fn get_page_to_read(
    app: &AppContext,
    topic_data: &TopicData,
    page_id: &MessagePageId,
) -> Result<Arc<MessagesPage>, OperationError> {
    loop {
        let page = topic_data.pages_list.get(page_id.value).await;

        if let Some(page) = page {
            return Ok(page);
        };

        let page = app
            .open_uncompressed_page_storage_if_exists(topic_data.topic_id.as_str(), &page_id.value)
            .await;

        let page = if let Some(storage) = page {
            MessagesPage::create_uncompressed(page_id.value, storage).await
        } else {
            MessagesPage::create_as_empty(page_id.value)
        };

        topic_data
            .pages_list
            .add(page_id.value, Arc::new(page))
            .await;
    }
}
