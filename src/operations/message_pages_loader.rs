use std::{collections::BTreeMap, sync::Arc};

use my_azure_page_blob_append::PageBlobAppendError;
use my_service_bus_shared::{protobuf_models::MessageProtobufModel, MessageId};

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
    let uncompressed_page_load_result = load_uncompressed_page(
        app.clone(),
        topic_data.topic_id.as_str(),
        page_id,
        page_id_current,
    )
    .await;

    match uncompressed_page_load_result {
        Ok(page_data) => {
            return page_data;
        }
        Err(err) => {
            println!(
                "Can not load from uncompressed page {}/{}. Err: {:?}",
                topic_data.topic_id, page_id.value, err
            );

            let result = load_compressed_page(app.as_ref(), topic_data.as_ref(), page_id).await;
            if let Some(result) = result {
                return result;
            }

            if page_id_current {
                return init_current_blob(app.clone(), topic_data.topic_id.as_str(), page_id).await;
            }

            return MessagesPageData::new_blank();
        }
    }
}

async fn load_uncompressed_page(
    app: Arc<AppContext>,
    topic_id: &str,
    page_id: MessagePageId,
    page_us_current: bool,
) -> Result<MessagesPageData, PageBlobAppendError> {
    let mut messages_page_blob =
        MessagesPageBlob::new(topic_id.to_string(), page_id.clone(), app.clone());

    let messages = messages_page_blob.load(page_us_current).await?;

    let as_tree_map = to_tree_map(messages);

    let page_data =
        MessagesPageData::restored_uncompressed(page_id.clone(), as_tree_map, messages_page_blob);

    return Ok(page_data);
}

async fn load_compressed_page(
    app: &AppContext,
    topic_data: &TopicData,
    page_id: MessagePageId,
) -> Option<MessagesPageData> {
    let read_result = topic_data.pages_cluster.read(page_id).await;

    match read_result {
        Ok(payload) => {
            if let Some(zip_archive) = payload {
                return MessagesPageData::resored_compressed(page_id, zip_archive);
            }

            return None;
        }
        Err(err) => {
            app.logs
                .add_error(
                    Some(topic_data.topic_id.as_str()),
                    "Load compressed page",
                    "Can not restore from compressed page",
                    format!("{:?}", err),
                )
                .await;

            return None;
        }
    }
}

async fn init_current_blob(
    app: Arc<AppContext>,
    topic_id: &str,
    page_id: MessagePageId,
) -> MessagesPageData {
    todo!("Not Implemented yet");
}

fn to_tree_map(msgs: Vec<MessageProtobufModel>) -> BTreeMap<MessageId, MessageProtobufModel> {
    let mut result = BTreeMap::new();

    for msg in msgs {
        result.insert(msg.message_id, msg);
    }

    result
}
