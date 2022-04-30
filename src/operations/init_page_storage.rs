use std::collections::HashMap;

use my_service_bus_shared::page_id::PageId;

use crate::{
    app::AppContext,
    message_pages::UncompressedPage,
    uncompressed_page_storage::{UncompressedPageStorage, UncompressedStorageError},
};

pub async fn init(
    app: &AppContext,
    topic_id: &str,
    messages_page: &UncompressedPage,
    uncomopressed_storages: &mut HashMap<PageId, UncompressedPageStorage>,
    create_if_not_exists: bool,
) -> Result<(), UncompressedStorageError> {
    if uncomopressed_storages.contains_key(&messages_page.page_id) {
        return Ok(());
    }

    let page_storage =
        open_uncompressed(app, topic_id, messages_page.page_id, create_if_not_exists).await?;

    uncomopressed_storages.insert(messages_page.page_id, page_storage);

    Ok(())
}

async fn open_uncompressed(
    app: &AppContext,
    topic_id: &str,
    page_id: PageId,
    create_if_not_exists: bool,
) -> Result<UncompressedPageStorage, UncompressedStorageError> {
    let open_result = app
        .open_uncompressed_page_storage(topic_id, page_id, create_if_not_exists)
        .await;

    if open_result.is_none() {
        return Err(UncompressedStorageError::FileNotFound);
    }

    Ok(open_result.unwrap())
}
