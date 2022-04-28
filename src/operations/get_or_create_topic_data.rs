use std::sync::Arc;

use crate::{app::AppContext, topic_data::TopicData};

pub async fn get_or_create_topic_data(app: &AppContext, topic_id: &str) -> Arc<TopicData> {
    todo!("Implement");
}
