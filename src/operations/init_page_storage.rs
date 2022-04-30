use std::collections::HashMap;

use my_service_bus_shared::page_id::PageId;

use crate::{
    app::AppContext,
    message_pages::UncompressedPage,
    uncompressed_page_storage::{UncompressedPageStorage, UncompressedStorageError},
};

pub async fn init<'s>(
    app: &AppContext,
    topic_id: &str,
    messages_page: &UncompressedPage,
    uncomopressed_storages: &'s mut HashMap<PageId, UncompressedPageStorage>,
    create_if_not_exists: bool,
) -> Result<&'s mut UncompressedPageStorage, UncompressedStorageError> {
    if uncomopressed_storages.contains_key(&messages_page.page_id) {
        let result = uncomopressed_storages
            .get_mut(&messages_page.page_id)
            .unwrap();

        return Ok(result);
    }

    let page_storage = if create_if_not_exists {
        app.open_or_create_uncompressed_page_storage(topic_id, &messages_page.page_id)
            .await
    } else {
        let result = app
            .open_uncompressed_page_storage_if_exists(topic_id, &messages_page.page_id)
            .await;

        if result.is_none() {
            return Err(UncompressedStorageError::FileNotFound);
        }

        result.unwrap()
    };

    uncomopressed_storages.insert(messages_page.page_id, page_storage);

    let result = uncomopressed_storages
        .get_mut(&messages_page.page_id)
        .unwrap();

    return Ok(result);
}
