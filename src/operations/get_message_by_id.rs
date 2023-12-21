use std::sync::Arc;

use my_service_bus::abstractions::MessageId;
use my_service_bus::shared::protobuf_models::MessageProtobufModel;

use crate::app::AppContext;

use super::OperationError;

pub async fn get_message_by_id(
    app: &AppContext,
    topic_id: &str,
    message_id: MessageId,
) -> Result<Option<Arc<MessageProtobufModel>>, OperationError> {
    let topic_data = super::topics::get_topic(app, topic_id).await?;

    let sub_page_id = message_id.into();

    let page = super::get_page_to_read(app, topic_data.as_ref(), sub_page_id).await;

    Ok(page.get_message(message_id).await)
}
