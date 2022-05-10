use std::sync::Arc;

use my_service_bus_shared::{protobuf_models::MessageProtobufModel, MessageId};

use crate::{
    app::AppContext,
    message_pages::{CompressedClusterId, CompressedPageId, MessagePageId},
};

use super::OperationError;

pub async fn get_message_by_id(
    app: &AppContext,
    topic_id: &str,
    message_id: MessageId,
) -> Result<Option<Arc<MessageProtobufModel>>, OperationError> {
    let topic_data = super::topics::get_topic(app, topic_id).await?;

    let page_id = MessagePageId::from_message_id(message_id);

    if let Some(page) =
        super::get_uncompressed_page_to_read(app, topic_data.as_ref(), &page_id).await
    {
        let result = page.get_message(message_id).await;
        if result.is_some() {
            return Ok(result);
        }
    }

    let cluster_id = CompressedClusterId::from_message_id(message_id);

    if let Some(compressed_cluster) =
        super::get_compressed_cluster_to_read(app, topic_data.as_ref(), &cluster_id).await
    {
        let compressed_page_id = CompressedPageId::from_message_id(message_id);

        if let Some(mut messages) = compressed_cluster
            .get_compressed_page_messages(&compressed_page_id)
            .await?
        {
            if let Some(result) = messages.remove(&message_id) {
                return Ok(Some(Arc::new(result)));
            }
        }
    }

    Ok(None)
}
