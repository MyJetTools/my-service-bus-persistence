use std::sync::Arc;

use crate::{
    app::{AppContext, TopicData},
    message_pages::{MessagePageId, MessagesPage},
};

pub async fn get_or_restore(
    app: Arc<AppContext>,
    topic_data: Arc<TopicData>,
    page_id: MessagePageId,
    is_current_page: bool,
) -> Arc<MessagesPage> {
    let page = topic_data.try_get_or_create_uninitialized(page_id).await;

    {
        let mut page_write_access = page.data.lock().await;

        if page_write_access.is_initialized() {
            return page.clone();
        }

        let page =
            super::message_pages_loader::load_page(app, topic_data, page_id, is_current_page).await;
        *page_write_access = page;
    }

    return page;
}
