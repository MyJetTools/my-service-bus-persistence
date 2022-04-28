use std::{collections::HashMap, sync::Arc};

use my_service_bus_shared::page_id::PageId;

use crate::{
    app::AppContext, message_pages::MessagesPage, topic_data::TopicData,
    uncompressed_page_storage::UncompressedPageStorage,
};

pub async fn get_or_restore_page(
    app: &AppContext,
    topic_data: &TopicData,
    page_id: PageId,
) -> Arc<MessagesPage> {
    loop {
        let page = topic_data.pages_list.get(page_id).await;

        if let Some(page) = page {
            if page.is_initialized() {
                return page;
            }

            let uncompressed_page = page.unwrap_as_uncompressed_page();

            let mut storage = topic_data.storages.lock().await;

            crate::operations::init_page_storage::init(
                app,
                topic_data.topic_id.as_str(),
                uncompressed_page,
                &mut storage,
                false,
            )
            .await
            .unwrap();

            return page;
        };

        let mut storages = topic_data.storages.lock().await;

        if let Some(page) =
            try_restore_from_uncompressed(app, topic_data, page_id, &mut storages).await
        {
            return page;
        }

        if let Some(page) = try_restore_from_compressed(app, topic_data, page_id).await {
            return page;
        }

        let messages_page = Arc::new(MessagesPage::create_as_empty(page_id));

        topic_data
            .pages_list
            .add(page_id, messages_page.clone())
            .await;

        return messages_page;
    }
}

async fn try_restore_from_uncompressed(
    app: &AppContext,
    topic_data: &TopicData,
    page_id: PageId,
    uncomopressed_storages: &mut HashMap<PageId, UncompressedPageStorage>,
) -> Option<Arc<MessagesPage>> {
    todo!("Implement")
}

async fn try_restore_from_compressed(
    app: &AppContext,
    topic_data: &TopicData,
    page_id: PageId,
) -> Option<Arc<MessagesPage>> {
    todo!("Implement")
}
