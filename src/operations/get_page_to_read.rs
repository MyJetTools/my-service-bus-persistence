use std::sync::Arc;

use crate::{
    app::AppContext,
    message_pages::{MessagePageId, MessagesPage},
    topic_data::TopicData,
};

use super::OperationError;

pub async fn get_page_to_read(
    app: &AppContext,
    topic_data: &TopicData,
    page_id: &MessagePageId,
) -> Result<Option<Arc<MessagesPage>>, OperationError> {
    todo!("Implement");
}
