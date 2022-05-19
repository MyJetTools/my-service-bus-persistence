use std::sync::Arc;

use my_service_bus_shared::{page_id::PageId, protobuf_models::MessageProtobufModel, MessageId};

use crate::{app::AppContext, topic_data::TopicData, uncompressed_page::UncompressedPage};

pub async fn new_messages(
    app: &AppContext,
    topic_data: &TopicData,
    page_id: PageId,
    messages: Vec<MessageProtobufModel>,
) -> Arc<UncompressedPage> {
    let max_mesage_id = get_max_message_id(&messages);
    let page = crate::operations::get_page_to_publish_messages(app, topic_data, page_id).await;

    crate::operations::index_by_minute::new_messages(app, topic_data, messages.as_slice()).await;

    page.new_messages(messages).await;

    topic_data.update_max_message_id(max_mesage_id);

    page
}

fn get_max_message_id(messages: &[MessageProtobufModel]) -> MessageId {
    let mut result = None;

    for msg in messages {
        if let Some(message_id) = result {
            if message_id < msg.message_id {
                result = Some(msg.message_id);
            }
        } else {
            result = Some(msg.message_id);
        }
    }

    result.unwrap()
}
