use std::sync::Arc;

use my_service_bus_shared::{protobuf_models::MessageProtobufModel, MessageId};

use crate::{app::AppContext, message_pages::MessagePageId};

use super::OperationError;

pub async fn get_message(
    app: Arc<AppContext>,
    topic_id: &str,
    message_id: MessageId,
) -> Result<Option<MessageProtobufModel>, OperationError> {
    let topic_data = super::topics::get_topic(app.as_ref(), topic_id).await?;

    let page_id = MessagePageId::from_message_id(message_id);

    let current_page_id = app.get_current_page_id(topic_id).await.unwrap();

    let page = super::pages::get_or_restore(
        app,
        topic_data,
        page_id,
        page_id.value >= current_page_id.value,
    )
    .await;

    let message = page.get_message(message_id).await?;

    Ok(message)
}
