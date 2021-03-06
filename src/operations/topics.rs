use std::sync::Arc;

use crate::{app::AppContext, topic_data::TopicData};

use super::OperationError;

pub async fn get_topic(app: &AppContext, topic_id: &str) -> Result<Arc<TopicData>, OperationError> {
    let result = app.topics_list.get(topic_id).await;

    match result {
        Some(topic) => Ok(topic),
        None => Err(OperationError::TopicNotFound(topic_id.to_string())),
    }
}
