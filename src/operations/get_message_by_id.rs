use my_service_bus_shared::{protobuf_models::MessageProtobufModel, MessageId};

use crate::{
    app::AppContext,
    message_pages::{MessagePageId, MessagesPage, UncompressedPage},
    topic_data::TopicData,
};

use super::OperationError;

pub async fn get_message_by_id(
    app: &AppContext,
    topic_id: &str,
    message_id: MessageId,
) -> Result<Option<MessageProtobufModel>, OperationError> {
    let topic_data = super::topics::get_topic(app, topic_id).await?;

    let page_id = MessagePageId::from_message_id(message_id);

    let page = super::get_page_to_read(app, topic_data.as_ref(), &page_id).await?;

    let page = page.as_ref();
    match page {
        MessagesPage::Uncompressed(page) => {
            if let Some(content) = page.get_message(message_id).await {
                return Ok(Some(content));
            }

            let result = read_message_from_blob(app, topic_data.as_ref(), page, message_id).await?;
            return Ok(result);
        }
        MessagesPage::Empty(_) => return Ok(None),
    }
}

pub async fn read_message_from_blob(
    app: &AppContext,
    topic_data: &TopicData,
    page: &UncompressedPage,
    message_id: MessageId,
) -> Result<Option<MessageProtobufModel>, OperationError> {
    let content = page.read_content(message_id).await;

    if content.is_none() {
        return Ok(None);
    }

    let message = match prost::Message::decode(content.unwrap().as_slice()) {
        Ok(message) => message,
        Err(err) => {
            app.logs.add_error(
                Some(topic_data.topic_id.as_str()),
                "read_message_from_blob",
                format!("Can not decode message from blob: {:?}", err),
                format!("MessageId: {}", message_id),
            );
            return Err(OperationError::ProtobufDecodeError(err));
        }
    };

    let result = page.restore_message(message).await;

    Ok(Some(result))
}
