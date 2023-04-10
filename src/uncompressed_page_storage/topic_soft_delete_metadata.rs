use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct TopicSoftDeleteMetadataBlobModel {
    pub message_id: i64,
    pub delete_after: String,
}
