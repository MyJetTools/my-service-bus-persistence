use std::sync::Arc;

use my_service_bus_shared::{protobuf_models::MessageProtobufModel, MessageId};

use crate::{app::AppContext, uncompressed_page::UncompressedPageId};

use super::{
    read_page::{MessagesReader, ReadCondition},
    OperationError,
};

pub async fn get_message_by_id(
    app: &Arc<AppContext>,
    topic_id: &str,
    message_id: MessageId,
) -> Result<Option<MessageProtobufModel>, OperationError> {
    let topic_data = super::topics::get_topic(app, topic_id).await?;

    let page_id = UncompressedPageId::from_message_id(message_id);

    let mut messages_reader = MessagesReader::new(
        app.clone(),
        topic_data,
        ReadCondition::SingleMessage(message_id),
    );
    let result = messages_reader.get_message().await;
    Ok(result)
}
