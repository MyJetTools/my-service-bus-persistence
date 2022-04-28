use std::collections::HashMap;

use my_service_bus_shared::{page_id::PageId, protobuf_models::MessageProtobufModel};

use crate::{
    app::AppContext,
    message_pages::UncompressedMessagesPage,
    uncompressed_page_storage::{UncompressedPageStorage, UncompressedStorageError},
};

pub async fn init(
    app: &AppContext,
    topic_id: &str,
    messages_page: &UncompressedMessagesPage,
    uncomopressed_storages: &mut HashMap<PageId, UncompressedPageStorage>,
    create_if_not_exists: bool,
) -> Result<(), UncompressedStorageError> {
    if uncomopressed_storages.contains_key(&messages_page.page_id) {
        return Ok(());
    }

    let mut page_storage =
        open_uncompressed(app, topic_id, messages_page.page_id, create_if_not_exists).await?;

    let loaded_pages = load_page(app, &mut page_storage, topic_id).await;

    let mut pages = messages_page.pages.write().await;
    pages.restore(loaded_pages);

    messages_page
        .initialized
        .store(true, std::sync::atomic::Ordering::SeqCst);

    uncomopressed_storages.insert(messages_page.page_id, page_storage);

    Ok(())
}

async fn open_uncompressed(
    app: &AppContext,
    topic_id: &str,
    page_id: PageId,
    create_if_not_exists: bool,
) -> Result<UncompressedPageStorage, UncompressedStorageError> {
    match app.open_uncompressed_page_storage(topic_id, page_id).await {
        Ok(result) => Ok(result),
        Err(err) => match err {
            UncompressedStorageError::FileNotFound => {
                let result = app
                    .create_uncompressed_page_storage(topic_id, page_id)
                    .await?;

                return Ok(result);
            }
            _ => Err(err),
        },
    }
}

async fn load_page(
    app: &AppContext,
    blob: &mut UncompressedPageStorage,
    topic_id: &str,
) -> Vec<MessageProtobufModel> {
    let mut attempt_no = 0;
    loop {
        match blob
            .init_and_load_messages(app.settings.max_message_size)
            .await
        {
            Ok(result) => return result,
            Err(err) => match err {
                UncompressedStorageError::OtherError(err) => {
                    app.logs.add_error(
                        Some(topic_id),
                        "load_uncompressed_page",
                        format!("Can not load compressed page. Attempt: #{}", attempt_no),
                        format!("{:?}", err),
                    );

                    if attempt_no > 5 {
                        panic!("Failed to load page blob: {:?}", err);
                    }

                    attempt_no += 1;
                }
                UncompressedStorageError::Corrupted => {
                    reset_file(blob).await;
                }
                UncompressedStorageError::FileNotFound => {
                    todo!("Handle case not found");
                }
            },
        }
    }
}

async fn reset_file(blob: &UncompressedPageStorage) {
    todo!("Implement")
}
