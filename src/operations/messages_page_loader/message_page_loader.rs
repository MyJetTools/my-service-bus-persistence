use std::{collections::BTreeMap, sync::Arc};

use crate::{
    app::{AppContext, TopicData},
    azure_storage::messages_page_blob::MessagesPageBlob,
    message_pages::{MessagePageId, MessagesPageData},
};

pub async fn load_page(
    app: Arc<AppContext>,
    topic_data: Arc<TopicData>,
    page_id: MessagePageId,
    page_id_current: bool,
) -> MessagesPageData {
    if page_id_current {
        load_current_page(app, topic_data, page_id).await
    } else {
        load_page_from_the_past(app, topic_data, page_id).await
    }
}

async fn load_current_page(
    app: Arc<AppContext>,
    topic_data: Arc<TopicData>,
    page_id: MessagePageId,
) -> MessagesPageData {
    let uncompressed_page_load_result = super::uncompressed_page_loader::load(
        page_id,
        true,
        MessagesPageBlob::new(
            topic_data.topic_id.to_string(),
            page_id.clone(),
            app.clone(),
        ),
    )
    .await;

    match uncompressed_page_load_result {
        Ok(page_data) => {
            return page_data;
        }
        Err(err) => {
            println!(
                "Can not load from uncompressed page {}/{}. Creating empry page... Err: {:?}",
                topic_data.topic_id, page_id.value, err
            );

            let mut page_blob = MessagesPageBlob::new(
                topic_data.topic_id.to_string(),
                page_id.clone(),
                app.clone(),
            );

            let result = page_blob.init_new_blob().await;

            if let Err(err) = result {
                panic!("Somehow we could not init new blob. Err:{:?}", err);
            }

            return MessagesPageData::restored_uncompressed(page_id, BTreeMap::new(), page_blob);
        }
    }
}

async fn load_page_from_the_past(
    app: Arc<AppContext>,
    topic_data: Arc<TopicData>,
    page_id: MessagePageId,
) -> MessagesPageData {
    let result =
        super::compressed_page_loader::load(app.as_ref(), topic_data.as_ref(), page_id).await;

    match result {
        Some(page) => return page,
        None => {
            return handle_fail_of_loading_compressed_page(app, topic_data, page_id).await;
        }
    }
}

async fn handle_fail_of_loading_compressed_page(
    app: Arc<AppContext>,
    topic_data: Arc<TopicData>,
    page_id: MessagePageId,
) -> MessagesPageData {
    let result = super::uncompressed_page_loader::load(
        page_id,
        false,
        MessagesPageBlob::new(
            topic_data.topic_id.to_string(),
            page_id.clone(),
            app.clone(),
        ),
    )
    .await;

    match result {
        Ok(page) => return page,
        Err(_) => {
            println!(
                "{}/{}. Could not load from compressed and uncompressed. Creating blank page",
                topic_data.topic_id, page_id.value
            );
            return MessagesPageData::new_blank();
        }
    }
}
