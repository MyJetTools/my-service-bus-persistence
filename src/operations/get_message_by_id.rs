use std::sync::Arc;

use my_service_bus_shared::{protobuf_models::MessageProtobufModel, MessageId};

use crate::{app::AppContext, message_pages::MessagePageId};

use super::OperationError;

pub async fn get_message_by_id(
    app: &AppContext,
    topic_id: &str,
    message_id: MessageId,
) -> Result<Option<Arc<MessageProtobufModel>>, OperationError> {
    let topic_data = super::topics::get_topic(app, topic_id).await?;

    let page_id = MessagePageId::from_message_id(message_id);

    let page = super::get_page_to_read(app, topic_data.as_ref(), &page_id).await;

    Ok(page.get_message(message_id).await)
}
